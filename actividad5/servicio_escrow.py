"""
ACTIVIDAD 5 - Servicio ESCROW (consumidor) sobre RabbitMQ + PostgreSQL (Neon).

- Consume la cola durable 'escrow.eventos.q' con ACK manual: el mensaje solo
  se borra de RabbitMQ despues de que el nuevo estado queda guardado (COMMIT)
  en PostgreSQL. Si el servicio se cae a mitad de camino, RabbitMQ vuelve a
  entregar el mensaje (entrega "al menos una vez").
- Idempotencia: el evento_id es llave primaria de eventos_procesados. Si llega
  un duplicado, el INSERT ... ON CONFLICT DO NOTHING no inserta nada, se
  confirma (ack) el mensaje y se ignora.
- Concurrencia: la fila de la transaccion se bloquea con SELECT ... FOR UPDATE,
  asi que se pueden correr varios servicios ESCROW a la vez sin que dos
  procesen la misma transaccion al mismo tiempo.
- Maquina de estados: un evento invalido para el estado actual se rechaza
  (reject sin requeue) y RabbitMQ lo manda a la cola de mensajes muertos.

Uso:
    set DATABASE_URL=postgresql://...neon.tech/neondb?sslmode=require   (Windows)
    export DATABASE_URL=...                                             (Linux/Mac)
    python servicio_escrow.py
"""
import asyncio
import json

import asyncpg

from bd import crear_pool, destino
from comun import Q_ESCROW, conectar, declarar_topologia, log, publicar

TRANSICIONES = {
    "SOLICITUD_PAGO":      ("CREADA", "INICIADA"),
    "PAGO_APROBADO":       ("INICIADA", "PAGO_RETENIDO"),
    "PRODUCTO_DESPACHADO": ("PAGO_RETENIDO", "PRODUCTO_DESPACHADO"),
    "ENTREGA_CONFIRMADA":  ("PRODUCTO_DESPACHADO", "ENTREGA_CONFIRMADA"),
}


ERRORES_BD = (OSError, asyncpg.PostgresConnectionError, asyncpg.InterfaceError,
              asyncpg.CannotConnectNowError)


class Duplicado(Exception):
    pass


class Invalido(Exception):
    pass


async def procesar(con, ev):
    """Aplica un evento dentro de UNA transaccion de BD. Devuelve las notificaciones a publicar."""
    tipo, tx_id, ev_id = ev["tipo"], ev["transaccion_id"], ev["evento_id"]
    notificaciones = []

    async with con.transaction():
        # ---- Idempotencia ----
        nuevo_evento = await con.fetchval(
            "INSERT INTO eventos_procesados(evento_id, transaccion_id, tipo) VALUES ($1,$2,$3) "
            "ON CONFLICT (evento_id) DO NOTHING RETURNING evento_id", ev_id, tx_id, tipo)
        if nuevo_evento is None:
            raise Duplicado()

        # ---- Estado actual (fila bloqueada hasta el COMMIT) ----
        fila = await con.fetchrow(
            "SELECT estado, valor, vendedor, comprador FROM transacciones WHERE id=$1 FOR UPDATE", tx_id)
        estado = fila["estado"] if fila else "CREADA"

        # ---- Validacion de la maquina de estados ----
        if tipo not in TRANSICIONES or estado != TRANSICIONES[tipo][0]:
            raise Invalido(f"invalido en estado {estado}")       # ROLLBACK: el evento no queda registrado
        nuevo = TRANSICIONES[tipo][1]

        if tipo == "SOLICITUD_PAGO":
            await con.execute(
                "INSERT INTO transacciones(id, comprador, vendedor, valor, estado) VALUES ($1,$2,$3,$4,$5)",
                tx_id, ev["comprador"], ev["vendedor"], ev["valor"], nuevo)
            fila = {"valor": ev["valor"], "vendedor": ev["vendedor"], "comprador": ev["comprador"]}
            notificaciones.append(("pasarela.cobro", {"transaccion_id": tx_id, "valor": ev["valor"]}))
        else:
            await con.execute("UPDATE transacciones SET estado=$1, actualizado=now() WHERE id=$2", nuevo, tx_id)

        await con.execute(
            "INSERT INTO historial(transaccion_id, estado_anterior, estado_nuevo, evento_id) VALUES ($1,$2,$3,$4)",
            tx_id, estado, nuevo, ev_id)

        if tipo == "PAGO_APROBADO":
            notificaciones.append(("vendedor.pago_retenido", {"transaccion_id": tx_id, "vendedor": fila["vendedor"]}))
        if tipo == "PRODUCTO_DESPACHADO":
            notificaciones.append(("comprador.producto_despachado",
                                   {"transaccion_id": tx_id, "comprador": fila["comprador"]}))

        # ---- Regla fundamental: liberar solo con la entrega confirmada ----
        if nuevo == "ENTREGA_CONFIRMADA":
            await con.execute("UPDATE transacciones SET estado='DINERO_LIBERADO', actualizado=now() WHERE id=$1",
                              tx_id)
            await con.execute(
                "INSERT INTO historial(transaccion_id, estado_anterior, estado_nuevo, evento_id) "
                "VALUES ($1,$2,'DINERO_LIBERADO',$3)", tx_id, nuevo, ev_id)
            notificaciones.append(("vendedor.dinero_liberado",
                                   {"transaccion_id": tx_id, "vendedor": fila["vendedor"], "valor": fila["valor"]}))

    return estado, nuevo, fila, notificaciones


async def main():
    pool = await crear_pool()
    conexion = await conectar()
    canal = await conexion.channel()
    await canal.set_qos(prefetch_count=10)          # maximo 10 mensajes sin confirmar
    ex = await declarar_topologia(canal)
    cola = await canal.get_queue(Q_ESCROW)

    pendientes = await pool.fetchval("SELECT COUNT(*) FROM transacciones WHERE estado NOT IN "
                                     "('DINERO_LIBERADO','CANCELADA','REEMBOLSADA')")
    log("ESCROW", f"Servicio iniciado. BD PostgreSQL: {destino()}. "
                  f"Transacciones abiertas recuperadas: {pendientes}")

    async with cola.iterator() as mensajes:
        async for msg in mensajes:
            ev = json.loads(msg.body)
            tipo, tx_id, ev_id = ev["tipo"], ev["transaccion_id"], ev["evento_id"]

            # Si la BD no responde se reintenta ESTE mismo mensaje (sin devolverlo a la
            # cola) hasta que vuelva: los mensajes siguientes esperan detras y asi se
            # conserva el orden de los eventos.
            espera = 1
            while True:
                try:
                    async with pool.acquire() as con:
                        estado, nuevo, fila, notificaciones = await procesar(con, ev)
                    resultado = "ok"
                except Duplicado:
                    resultado = "duplicado"
                except Invalido as e:
                    resultado, motivo = "invalido", str(e)
                except ERRORES_BD as e:
                    log("ESCROW", f"BD no disponible ({type(e).__name__}) procesando {tipo} {tx_id}; "
                                  f"reintento en {espera} s")
                    await asyncio.sleep(espera)
                    espera = min(espera * 2, 8)        # espera exponencial: 1, 2, 4, 8 s
                    continue
                except Exception as e:                 # error inesperado: no bloquear la cola
                    resultado, motivo = "invalido", f"error inesperado {e!r}"
                break

            if resultado == "duplicado":
                log("ESCROW", f"{tipo} {tx_id} [{ev_id}] DUPLICADO -> ignorado")
                await msg.ack()
                continue
            if resultado == "invalido":
                log("ESCROW", f"RECHAZADO {tipo} {tx_id}: {motivo} -> DLQ")
                await msg.reject(requeue=False)        # RabbitMQ lo envia a escrow.eventos.dlq
                continue

            log("ESCROW", f"{tipo:<20} {tx_id}  {estado} -> {nuevo}")
            if nuevo == "PAGO_RETENIDO":
                log("ESCROW", f"Dinero de {tx_id} retenido en garantia (${fila['valor']:,})")
            if nuevo == "ENTREGA_CONFIRMADA":
                log("ESCROW", f"*** DINERO LIBERADO a {fila['vendedor']}: ${fila['valor']:,} ({tx_id})")

            for rk, cuerpo in notificaciones:
                await publicar(ex["notif"], rk, cuerpo)
            await msg.ack()                            # recien ahora RabbitMQ borra el mensaje


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
