"""
Laboratorio 8 - Ingenieria de Software 2 - Universidad El Bosque
ACTIVIDAD 3: Multiples transacciones ESCROW simultaneas.

Parte del codigo base del profesor (EscrowV1.py) y lo extiende a:
  - 10 compradores, 10 vendedores, 10 transacciones ESCROW
  - valores diferentes para cada transaccion
  - una sola cola de mensajes (asyncio.Queue) compartida
  - un solo servicio ESCROW consumidor que procesa los eventos de todas
    las transacciones en el orden en que llegan

Cambios frente a la V1:
  1. 'transaccion' (un dict) -> 'transacciones' (dict de dicts por id).
  2. Cada evento lleva 'transaccion_id' para que el ESCROW sepa a cual
     transaccion pertenece.
  3. Cada transaccion tiene un 'estado' (ciclo de vida del enunciado):
     INICIADA -> PAGO_RETENIDO -> PRODUCTO_DESPACHADO -> ENTREGA_CONFIRMADA
     -> DINERO_LIBERADO
  4. Notificaciones del ESCROW hacia los participantes (asyncio.Event):
     el vendedor solo despacha cuando el ESCROW le avisa que el pago esta
     retenido, y el comprador solo confirma cuando el producto le llega.
     Asi se respeta el flujo de las reglas de negocio aunque los tiempos
     sean aleatorios (en la V1 el orden dependia de los sleep fijos).
  5. Tiempos aleatorios (con semilla) para que la concurrencia sea visible.
  6. Bitacora en CSV con marca de tiempo de cada evento -> graficas.

Ejecucion:
    python escrow_multi.py              # 10 transacciones (por defecto)
    python escrow_multi.py --n 100      # prueba de carga
    python escrow_multi.py --seed 7     # otra corrida reproducible
"""

import argparse
import asyncio
import csv
import random
import time
from datetime import datetime

# -----------------------------------------
# CONFIGURACION
# -----------------------------------------

ESTADOS = [
    "INICIADA",
    "PAGO_RETENIDO",
    "PRODUCTO_DESPACHADO",
    "ENTREGA_CONFIRMADA",
    "DINERO_LIBERADO",
]

cola_eventos: asyncio.Queue = None          # se crea dentro del event loop
transacciones = {}                          # id -> estado de la transaccion
notificaciones = {}                         # id -> {"pago_retenido": Event, "producto_entregado": Event}
bitacora = []                               # filas para el CSV de evidencias
T0 = time.perf_counter()
VERBOSO = True
WORKERS = 1
COSTO_MS = 0          # tiempo que tarda el ESCROW en procesar cada evento (validaciones, BD...)


def t_rel():
    """Segundos transcurridos desde el inicio de la simulacion."""
    return time.perf_counter() - T0


def registrar(mensaje):
    if VERBOSO:
        hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{hora} | t={t_rel():6.3f}s] {mensaje}")


def anotar(tx_id, actor, accion, detalle=""):
    bitacora.append({
        "t": round(t_rel(), 4),
        "transaccion": tx_id,
        "actor": actor,
        "accion": accion,
        "tam_cola": cola_eventos.qsize(),
        "detalle": detalle,
    })


async def publicar(evento):
    """Productor: deposita el evento en la cola y sigue (no espera al ESCROW)."""
    evento["t_envio"] = t_rel()
    await cola_eventos.put(evento)
    anotar(evento["transaccion_id"], evento["origen"], "ENVIA_" + evento["tipo"])


# -----------------------------------------
# PARTICIPANTE: COMPRADOR
# -----------------------------------------

async def comprador(num, tx_id):
    nombre = f"COMPRADOR-{num:02d}"
    await asyncio.sleep(random.uniform(0.0, 1.0))        # llegan en momentos distintos
    registrar(f"{nombre}: Iniciando transaccion {tx_id}")
    await publicar({"tipo": "SOLICITUD_PAGO", "origen": nombre, "transaccion_id": tx_id})

    # Espera (sin bloquear a nadie) a que el producto le llegue
    await notificaciones[tx_id]["producto_entregado"].wait()
    await asyncio.sleep(random.uniform(0.2, 1.5))        # tiempo en revisar el producto
    registrar(f"{nombre}: Producto recibido ({tx_id})")
    await publicar({"tipo": "ENTREGA_CONFIRMADA", "origen": nombre, "transaccion_id": tx_id})


# -----------------------------------------
# PARTICIPANTE: PASARELA DE PAGO
# -----------------------------------------

async def pasarela_pago(tx_id):
    await asyncio.sleep(random.uniform(0.5, 2.0))        # latencia del banco
    registrar(f"PASARELA: Pago aprobado ({tx_id})")
    await publicar({"tipo": "PAGO_APROBADO", "origen": "PASARELA", "transaccion_id": tx_id})


# -----------------------------------------
# PARTICIPANTE: VENDEDOR
# -----------------------------------------

async def vendedor(num, tx_id):
    nombre = f"VENDEDOR-{num:02d}"
    # El vendedor recibe la notificacion del pago retenido (regla de negocio)
    await notificaciones[tx_id]["pago_retenido"].wait()
    await asyncio.sleep(random.uniform(0.3, 1.5))        # empaca el producto
    registrar(f"{nombre}: Preparando despacho ({tx_id})")
    await publicar({"tipo": "PRODUCTO_DESPACHADO", "origen": nombre, "transaccion_id": tx_id})

    # Simula el transporte: el producto llega al comprador despues de un rato
    await asyncio.sleep(random.uniform(0.5, 2.0))
    notificaciones[tx_id]["producto_entregado"].set()


# -----------------------------------------
# SERVICIO ESCROW (consumidor)
# -----------------------------------------

async def servicio_escrow(nombre="ESCROW"):
    while True:
        evento = await cola_eventos.get()
        if COSTO_MS:
            await asyncio.sleep(COSTO_MS / 1000)          # simula trabajo del servicio
        tipo = evento["tipo"]
        tx_id = evento["transaccion_id"]
        tx = transacciones[tx_id]
        espera = t_rel() - evento["t_envio"]             # tiempo que estuvo en la cola
        tx["espera_en_cola"].append(espera)

        registrar(f"ESCROW: Procesando {tipo} de {tx_id} (estuvo {espera*1000:.1f} ms en cola)")

        if tipo == "SOLICITUD_PAGO":
            tx["estado"] = "INICIADA"
            tx["t_estados"]["INICIADA"] = t_rel()

        elif tipo == "PAGO_APROBADO":
            tx["pago_confirmado"] = True
            tx["estado"] = "PAGO_RETENIDO"
            tx["t_estados"]["PAGO_RETENIDO"] = t_rel()
            registrar(f"ESCROW: Dinero de {tx_id} retenido en garantia (${tx['valor']:,})")
            notificaciones[tx_id]["pago_retenido"].set()  # notifica al vendedor

        elif tipo == "PRODUCTO_DESPACHADO":
            tx["producto_despachado"] = True
            tx["estado"] = "PRODUCTO_DESPACHADO"
            tx["t_estados"]["PRODUCTO_DESPACHADO"] = t_rel()

        elif tipo == "ENTREGA_CONFIRMADA":
            tx["entrega_confirmada"] = True
            tx["estado"] = "ENTREGA_CONFIRMADA"
            tx["t_estados"]["ENTREGA_CONFIRMADA"] = t_rel()

        anotar(tx_id, "ESCROW", "PROCESA_" + tipo, f"espera_ms={espera*1000:.2f}")

        # Regla fundamental: solo se libera con pago + despacho + entrega confirmada
        if (
            tx["pago_confirmado"]
            and tx["producto_despachado"]
            and tx["entrega_confirmada"]
            and not tx["dinero_liberado"]
        ):
            tx["dinero_liberado"] = True
            tx["estado"] = "DINERO_LIBERADO"
            tx["t_estados"]["DINERO_LIBERADO"] = t_rel()
            registrar(f"ESCROW: DINERO LIBERADO AL {tx['vendedor']} -> ${tx['valor']:,} ({tx_id})")
            anotar(tx_id, "ESCROW", "DINERO_LIBERADO", f"valor={tx['valor']}")

        cola_eventos.task_done()


# -----------------------------------------
# PROGRAMA PRINCIPAL
# -----------------------------------------

def crear_transacciones(n):
    for i in range(1, n + 1):
        tx_id = f"ESCROW-{i:03d}"
        transacciones[tx_id] = {
            "id": tx_id,
            "comprador": f"COMPRADOR-{i:02d}",
            "vendedor": f"VENDEDOR-{i:02d}",
            "valor": random.randrange(50_000, 3_000_001, 10_000),   # valores diferentes
            "estado": "CREADA",
            "pago_confirmado": False,
            "producto_despachado": False,
            "entrega_confirmada": False,
            "dinero_liberado": False,
            "t_estados": {},
            "espera_en_cola": [],
        }
        notificaciones[tx_id] = {
            "pago_retenido": asyncio.Event(),
            "producto_entregado": asyncio.Event(),
        }


async def main(n, prefijo_salida):
    global cola_eventos, T0
    cola_eventos = asyncio.Queue()
    T0 = time.perf_counter()

    registrar(f"INICIANDO SISTEMA ESCROW con {n} transacciones simultaneas")
    crear_transacciones(n)

    # Uno o varios consumidores ESCROW leyendo de la misma cola
    consumidores = [asyncio.create_task(servicio_escrow(f"ESCROW-W{k}")) for k in range(WORKERS)]

    tareas = []
    for i, tx_id in enumerate(transacciones, start=1):
        tareas.append(asyncio.create_task(comprador(i, tx_id)))
        tareas.append(asyncio.create_task(pasarela_pago(tx_id)))
        tareas.append(asyncio.create_task(vendedor(i, tx_id)))

    await asyncio.gather(*tareas)
    await cola_eventos.join()
    duracion_total = t_rel()

    for c in consumidores:
        c.cancel()
    await asyncio.gather(*consumidores, return_exceptions=True)

    # ---------- RESUMEN ----------
    print("\nRESUMEN DE LAS TRANSACCIONES")
    print(f"{'ID':<11}{'Comprador':<15}{'Vendedor':<13}{'Valor':>13}  {'Estado final':<17}{'Duracion (s)':>12}")
    liberado_total = 0
    for tx in transacciones.values():
        dur = tx["t_estados"]["DINERO_LIBERADO"] - tx["t_estados"]["INICIADA"]
        liberado_total += tx["valor"] if tx["dinero_liberado"] else 0
        print(f"{tx['id']:<11}{tx['comprador']:<15}{tx['vendedor']:<13}{tx['valor']:>13,}  {tx['estado']:<17}{dur:>12.2f}")

    esperas = [e for tx in transacciones.values() for e in tx["espera_en_cola"]]
    print(f"\nTransacciones completadas : {sum(tx['dinero_liberado'] for tx in transacciones.values())}/{n}")
    print(f"Dinero liberado total     : ${liberado_total:,}")
    print(f"Duracion total simulacion : {duracion_total:.2f} s")
    print(f"Eventos procesados        : {len(esperas)}")
    print(f"Consumidores ESCROW       : {WORKERS}  (costo por evento {COSTO_MS} ms)")
    print(f"Espera en cola  prom/max  : {sum(esperas)/len(esperas)*1000:.2f} ms / {max(esperas)*1000:.2f} ms")

    # ---------- EVIDENCIAS ----------
    with open(f"{prefijo_salida}_bitacora.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=bitacora[0].keys())
        w.writeheader()
        w.writerows(bitacora)
    with open(f"{prefijo_salida}_transacciones.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "comprador", "vendedor", "valor", "estado"] + ESTADOS)
        for tx in transacciones.values():
            w.writerow([tx["id"], tx["comprador"], tx["vendedor"], tx["valor"], tx["estado"]]
                       + [round(tx["t_estados"].get(s, -1), 4) for s in ESTADOS])
    print(f"\nEvidencias guardadas en {prefijo_salida}_bitacora.csv y {prefijo_salida}_transacciones.csv")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=10, help="numero de transacciones")
    p.add_argument("--seed", type=int, default=2026)
    p.add_argument("--quiet", action="store_true", help="no imprimir cada evento")
    p.add_argument("--out", default="evidencias/10tx")
    p.add_argument("--costo-ms", type=float, default=0, help="costo de procesar cada evento en el ESCROW")
    p.add_argument("--workers", type=int, default=1, help="numero de consumidores ESCROW")
    a = p.parse_args()
    import os
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    COSTO_MS = a.costo_ms
    WORKERS = a.workers
    random.seed(a.seed)
    VERBOSO = not a.quiet
    asyncio.run(main(a.n, a.out))
