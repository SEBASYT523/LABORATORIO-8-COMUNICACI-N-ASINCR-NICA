"""
Laboratorio 8 - Ingenieria de Software 2 - Universidad El Bosque
ACTIVIDAD 4: Analisis de fallos en el sistema ESCROW.

Escenarios simulados:
  1. pasarela_caida          -> la pasarela de pago nunca responde
  2. pasarela_intermitente   -> la pasarela falla 2 veces y luego responde
  3. vendedor_no_despacha    -> el vendedor nunca envia el producto
  4. comprador_no_confirma   -> el comprador nunca confirma la recepcion
  5. evento_duplicado        -> el ESCROW recibe dos veces el mismo evento
  (normal                    -> flujo exitoso, como control)

Cada escenario se puede correr en dos modos:
  --sin-proteccion : la logica del codigo base del profesor (sin plazos, sin
                     idempotencia, sin validar transiciones de estado).
  (por defecto)    : con los mecanismos de recuperacion propuestos.

Mecanismos de recuperacion implementados:
  A. Plazos (timeouts) por estado: cuando una transaccion entra a un estado,
     el ESCROW programa un temporizador. Si al vencerse la transaccion sigue
     en el mismo estado, se publica un evento PLAZO_VENCIDO en la misma cola.
  B. Reintentos con espera exponencial (backoff) hacia la pasarela.
  C. Recordatorios al participante que no ha actuado.
  D. Compensacion: cancelacion + reembolso si el vendedor no despacha,
     reembolso automatico si un pago llega despues de cancelar.
  E. Liberacion por aceptacion tacita si el comprador no confirma (como hacen
     las plataformas reales), solo si el despacho esta registrado.
  F. Idempotencia: cada evento tiene un 'evento_id' unico; los ya procesados
     se descartan.
  G. Maquina de estados: un evento solo se acepta si es valido para el estado
     actual. Los invalidos van a una cola de mensajes muertos (DLQ).

En la simulacion los plazos son de segundos; en un sistema real serian horas
o dias (p. ej. 72 h para despachar, 7 dias para confirmar).

Uso:
    python escrow_fallos.py --escenario todos
    python escrow_fallos.py --escenario vendedor_no_despacha --sin-proteccion
"""

import argparse
import asyncio
import csv
import time
import uuid
from datetime import datetime

# -----------------------------------------
# PARAMETROS DE RECUPERACION
# -----------------------------------------

PLAZO_PAGO = 1.5             # s que se espera la respuesta de la pasarela
MAX_REINTENTOS_PAGO = 3      # reintentos antes de cancelar
BACKOFF_BASE = 0.5           # 0.5 s, 1 s, 2 s ...
PLAZO_DESPACHO = 2.0         # s que tiene el vendedor para despachar
PLAZO_CONFIRMACION = 2.0     # s que tiene el comprador para confirmar
MAX_RECORDATORIOS = 1
LIMITE_SIMULACION = 15.0     # corte de seguridad de la simulacion

ESTADOS_FINALES = {"DINERO_LIBERADO", "CANCELADA", "REEMBOLSADA"}

# Maquina de estados: evento -> estado en el que es valido
TRANSICIONES = {
    "SOLICITUD_PAGO":      ("CREADA", "INICIADA"),
    "PAGO_APROBADO":       ("INICIADA", "PAGO_RETENIDO"),
    "PRODUCTO_DESPACHADO": ("PAGO_RETENIDO", "PRODUCTO_DESPACHADO"),
    "ENTREGA_CONFIRMADA":  ("PRODUCTO_DESPACHADO", "ENTREGA_CONFIRMADA"),
}


class Simulacion:
    def __init__(self, escenario, protegido, valor=500_000):
        self.escenario = escenario
        self.protegido = protegido
        self.cola_eventos = asyncio.Queue()       # participantes -> ESCROW
        self.cola_pasarela = asyncio.Queue()      # ESCROW -> pasarela
        self.dlq = []                             # cola de mensajes muertos
        self.procesados = set()                   # evento_id ya procesados (idempotencia)
        self.t0 = time.perf_counter()
        self.lineas = []
        self.pago_retenido = asyncio.Event()      # notificacion al vendedor
        self.producto_entregado = asyncio.Event() # el producto llega al comprador
        self.tareas_plazo = []
        self.tx = {
            "id": "ESCROW-001", "valor": valor, "estado": "CREADA",
            # banderas del modelo original (se usan en modo sin proteccion)
            "pago_confirmado": False, "producto_despachado": False,
            "entrega_confirmada": False, "dinero_liberado": False,
            # contabilidad: aqui se ve el dano de un evento duplicado
            "saldo_en_custodia": 0, "pagado_al_vendedor": 0, "reembolsado_al_comprador": 0,
            "cobros_realizados": 0,
            "intentos_pago": 0, "recordatorios": 0, "historial": [],
        }

    # ---------- utilidades ----------
    def t(self):
        return time.perf_counter() - self.t0

    def log(self, msg):
        linea = f"[t={self.t():5.2f}s] {msg}"
        self.lineas.append(linea)
        print(linea)

    def cambiar_estado(self, nuevo, motivo=""):
        anterior = self.tx["estado"]
        self.tx["estado"] = nuevo
        self.tx["historial"].append((round(self.t(), 2), anterior, nuevo, motivo))
        self.log(f"ESCROW: estado {anterior} -> {nuevo}" + (f"  ({motivo})" if motivo else ""))
        if self.protegido and nuevo in ESTADOS_FINALES:
            for tp in self.tareas_plazo:
                tp.cancel()

    async def publicar(self, tipo, origen, evento_id=None, **extra):
        ev = {"evento_id": evento_id or str(uuid.uuid4())[:8], "tipo": tipo,
              "origen": origen, "transaccion_id": self.tx["id"], **extra}
        await self.cola_eventos.put(ev)
        return ev

    # ---------- mecanismo A: plazos ----------
    def programar_plazo(self, estado_esperado, segundos):
        if not self.protegido:
            return

        async def temporizador():
            await asyncio.sleep(segundos)
            await self.publicar("PLAZO_VENCIDO", "TEMPORIZADOR", estado_esperado=estado_esperado)

        self.tareas_plazo.append(asyncio.create_task(temporizador()))

    # =========================================
    # PARTICIPANTES
    # =========================================

    async def comprador(self):
        await asyncio.sleep(0.2)
        self.log("COMPRADOR: Iniciando transaccion y realizando pago")
        await self.publicar("SOLICITUD_PAGO", "COMPRADOR")

        await self.producto_entregado.wait()
        await asyncio.sleep(0.5)
        if self.escenario == "comprador_no_confirma":
            self.log("COMPRADOR: (recibio el producto pero NUNCA confirma)")
            return
        self.log("COMPRADOR: Producto recibido, confirmo la entrega")
        ev = await self.publicar("ENTREGA_CONFIRMADA", "COMPRADOR")
        if self.escenario == "evento_duplicado":
            await asyncio.sleep(0.05)
            self.log("RED: reenvia ENTREGA_CONFIRMADA (mismo evento_id)")
            await self.cola_eventos.put(dict(ev))

    async def pasarela_pago(self):
        fallos_restantes = 2 if self.escenario == "pasarela_intermitente" else 0
        while True:
            solicitud = await self.cola_pasarela.get()
            intento = solicitud["intento"]
            if self.escenario == "pasarela_caida":
                self.log(f"PASARELA: (intento {intento}) ... sin respuesta")
                continue
            if fallos_restantes > 0:
                fallos_restantes -= 1
                self.log(f"PASARELA: (intento {intento}) ... sin respuesta")
                continue
            await asyncio.sleep(0.6)
            self.log(f"PASARELA: Pago aprobado (intento {intento})")
            ev = await self.publicar("PAGO_APROBADO", "PASARELA")
            if self.escenario == "evento_duplicado":
                await asyncio.sleep(0.05)
                self.log("RED: reenvia PAGO_APROBADO (mismo evento_id, entrega 'al menos una vez')")
                await self.cola_eventos.put(dict(ev))

    async def vendedor(self):
        if self.protegido:
            await self.pago_retenido.wait()     # despacha solo si el ESCROW le avisa
        else:
            await asyncio.sleep(3)              # como en la V1: tiempo fijo
        if self.escenario == "vendedor_no_despacha":
            self.log("VENDEDOR: (recibio la notificacion pero NUNCA despacha)")
            return
        await asyncio.sleep(0.5)
        self.log("VENDEDOR: Producto despachado")
        await self.publicar("PRODUCTO_DESPACHADO", "VENDEDOR")
        await asyncio.sleep(0.8)                # transporte
        self.producto_entregado.set()

    # =========================================
    # SERVICIO ESCROW
    # =========================================

    async def servicio_escrow(self):
        while True:
            ev = await self.cola_eventos.get()
            try:
                if self.protegido:
                    self.procesar_protegido(ev)
                else:
                    self.procesar_sin_proteccion(ev)
            except Exception as e:                 # un error no tumba al consumidor
                self.dlq.append((ev, f"excepcion: {e}"))
                self.log(f"ESCROW: error procesando {ev['tipo']} -> DLQ")
            finally:
                self.cola_eventos.task_done()

    # ---------- Modo SIN proteccion (logica del codigo base) ----------
    def procesar_sin_proteccion(self, ev):
        tx, tipo = self.tx, ev["tipo"]
        self.log(f"ESCROW: Procesando {tipo} [{ev['evento_id']}]")
        if tipo == "SOLICITUD_PAGO":
            tx["estado"] = "INICIADA"
            self.cola_pasarela.put_nowait({"intento": 1})
        elif tipo == "PAGO_APROBADO":
            tx["pago_confirmado"] = True
            tx["saldo_en_custodia"] += tx["valor"]
            tx["cobros_realizados"] += 1
            tx["estado"] = "PAGO_RETENIDO"
            self.log(f"ESCROW: Dinero retenido. En custodia: ${tx['saldo_en_custodia']:,}")
        elif tipo == "PRODUCTO_DESPACHADO":
            tx["producto_despachado"] = True
            tx["estado"] = "PRODUCTO_DESPACHADO"
        elif tipo == "ENTREGA_CONFIRMADA":
            tx["entrega_confirmada"] = True
            tx["estado"] = "ENTREGA_CONFIRMADA"
        if (tx["pago_confirmado"] and tx["producto_despachado"]
                and tx["entrega_confirmada"] and not tx["dinero_liberado"]):
            tx["dinero_liberado"] = True
            self.liberar("condiciones cumplidas")

    # ---------- Modo CON proteccion ----------
    def procesar_protegido(self, ev):
        tx, tipo = self.tx, ev["tipo"]

        # F. Idempotencia
        if ev["evento_id"] in self.procesados:
            self.log(f"ESCROW: evento {tipo} [{ev['evento_id']}] DUPLICADO -> descartado")
            return
        self.procesados.add(ev["evento_id"])
        self.log(f"ESCROW: Procesando {tipo} [{ev['evento_id']}]")

        if tipo == "PLAZO_VENCIDO":
            self.manejar_plazo(ev["estado_esperado"])
            return

        # D. Pago tardio sobre una transaccion ya cancelada -> compensar
        if tipo == "PAGO_APROBADO" and tx["estado"] == "CANCELADA":
            tx["cobros_realizados"] += 1
            tx["reembolsado_al_comprador"] += tx["valor"]
            self.log("ESCROW: pago llego despues de cancelar -> reembolso automatico")
            return

        # G. Validacion de la maquina de estados
        if tipo not in TRANSICIONES or tx["estado"] != TRANSICIONES[tipo][0]:
            motivo = f"{tipo} no es valido en estado {tx['estado']}"
            self.dlq.append((ev, motivo))
            self.log(f"ESCROW: RECHAZADO ({motivo}) -> DLQ")
            return

        nuevo = TRANSICIONES[tipo][1]
        if tipo == "SOLICITUD_PAGO":
            self.cambiar_estado(nuevo)
            self.solicitar_cobro()
        elif tipo == "PAGO_APROBADO":
            tx["saldo_en_custodia"] += tx["valor"]
            tx["cobros_realizados"] += 1
            self.cambiar_estado(nuevo, f"en custodia ${tx['saldo_en_custodia']:,}")
            tx["recordatorios"] = 0
            self.pago_retenido.set()                       # notifica al vendedor
            self.programar_plazo("PAGO_RETENIDO", PLAZO_DESPACHO)
        elif tipo == "PRODUCTO_DESPACHADO":
            self.cambiar_estado(nuevo)
            tx["recordatorios"] = 0
            self.programar_plazo("PRODUCTO_DESPACHADO", PLAZO_CONFIRMACION)
        elif tipo == "ENTREGA_CONFIRMADA":
            self.cambiar_estado(nuevo)
            self.liberar("el comprador confirmo la recepcion")

    def solicitar_cobro(self):
        self.tx["intentos_pago"] += 1
        self.cola_pasarela.put_nowait({"intento": self.tx["intentos_pago"]})
        self.programar_plazo("INICIADA", PLAZO_PAGO)

    def manejar_plazo(self, estado_esperado):
        tx = self.tx
        if tx["estado"] != estado_esperado:
            self.log("ESCROW: plazo obsoleto (la transaccion ya avanzo) -> ignorado")
            return

        if estado_esperado == "INICIADA":                  # B. pasarela no responde
            if tx["intentos_pago"] < MAX_REINTENTOS_PAGO:
                espera = BACKOFF_BASE * 2 ** (tx["intentos_pago"] - 1)
                self.log(f"ESCROW: pasarela sin respuesta. Reintento {tx['intentos_pago'] + 1} "
                         f"en {espera:.1f} s (backoff)")

                async def reintentar():
                    await asyncio.sleep(espera)
                    if tx["estado"] == "INICIADA":
                        self.solicitar_cobro()
                self.tareas_plazo.append(asyncio.create_task(reintentar()))
            else:
                self.cambiar_estado("CANCELADA", f"pasarela sin respuesta tras {MAX_REINTENTOS_PAGO} intentos; "
                                                 "no se cobro nada al comprador")

        elif estado_esperado == "PAGO_RETENIDO":           # C/D. vendedor no despacha
            if tx["recordatorios"] < MAX_RECORDATORIOS:
                tx["recordatorios"] += 1
                self.log("ESCROW: RECORDATORIO al vendedor: debe despachar el producto")
                self.programar_plazo("PAGO_RETENIDO", PLAZO_DESPACHO)
            else:
                self.cambiar_estado("CANCELADA", "el vendedor no despacho dentro del plazo")
                tx["reembolsado_al_comprador"] += tx["saldo_en_custodia"]
                tx["saldo_en_custodia"] = 0
                self.cambiar_estado("REEMBOLSADA", f"se devuelven ${tx['reembolsado_al_comprador']:,} al comprador")

        elif estado_esperado == "PRODUCTO_DESPACHADO":     # C/E. comprador no confirma
            if tx["recordatorios"] < MAX_RECORDATORIOS:
                tx["recordatorios"] += 1
                self.log("ESCROW: RECORDATORIO al comprador: confirme la recepcion o abra una disputa")
                self.programar_plazo("PRODUCTO_DESPACHADO", PLAZO_CONFIRMACION)
            else:
                self.liberar("aceptacion tacita: el comprador no confirmo ni abrio disputa en el plazo")

    def liberar(self, motivo):
        tx = self.tx
        pago = min(tx["valor"], tx["saldo_en_custodia"])
        tx["pagado_al_vendedor"] += pago
        tx["saldo_en_custodia"] -= pago
        self.cambiar_estado("DINERO_LIBERADO", motivo)
        self.log(f"ESCROW: DINERO LIBERADO AL VENDEDOR ${pago:,}")

    # =========================================
    # EJECUCION DEL ESCENARIO
    # =========================================

    async def correr(self):
        modo = "CON proteccion" if self.protegido else "SIN proteccion (logica V1)"
        self.log(f"===== ESCENARIO: {self.escenario}  |  {modo} =====")
        tareas = [asyncio.create_task(self.servicio_escrow()),
                  asyncio.create_task(self.pasarela_pago()),
                  asyncio.create_task(self.comprador()),
                  asyncio.create_task(self.vendedor())]

        # Se espera hasta que la transaccion llegue a un estado final o se agote el limite
        atascada = False
        while self.tx["estado"] not in ESTADOS_FINALES:
            if self.t() > LIMITE_SIMULACION:
                atascada = True
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.3)        # deja llegar eventos rezagados (duplicados)
        for tsk in tareas + self.tareas_plazo:
            tsk.cancel()
        await asyncio.gather(*tareas, *self.tareas_plazo, return_exceptions=True)

        tx = self.tx
        if atascada:
            self.log(f"!!! Transaccion ATASCADA en {tx['estado']} tras {LIMITE_SIMULACION:.0f} s "
                     "(sin intervencion nunca terminaria)")
        # Consistente = todo lo cobrado se pago al vendedor o se devolvio; nada queda colgado
        inconsistente = (tx["cobros_realizados"] * tx["valor"]
                         != tx["pagado_al_vendedor"] + tx["reembolsado_al_comprador"]) \
            or tx["saldo_en_custodia"] > 0
        self.log(f"RESUMEN: estado={tx['estado']}  cobrado=${tx['cobros_realizados'] * tx['valor']:,}  "
                 f"al vendedor=${tx['pagado_al_vendedor']:,}  reembolsado=${tx['reembolsado_al_comprador']:,}  "
                 f"en custodia=${tx['saldo_en_custodia']:,}  DLQ={len(self.dlq)}")
        return {
            "escenario": self.escenario,
            "modo": "con_proteccion" if self.protegido else "sin_proteccion",
            "estado_final": tx["estado"] + (" (atascada)" if atascada else ""),
            "duracion_s": round(self.t(), 2),
            "cobrado": tx["cobros_realizados"] * tx["valor"],
            "pagado_vendedor": tx["pagado_al_vendedor"],
            "reembolsado": tx["reembolsado_al_comprador"],
            "en_custodia": tx["saldo_en_custodia"],
            "eventos_dlq": len(self.dlq),
            "dinero_consistente": "NO" if inconsistente or atascada else "SI",
        }


ESCENARIOS = ["normal", "pasarela_caida", "pasarela_intermitente",
              "vendedor_no_despacha", "comprador_no_confirma", "evento_duplicado"]


async def main(escenarios, modos):
    resultados = []
    for esc in escenarios:
        for protegido in modos:
            sim = Simulacion(esc, protegido)
            resultados.append(await sim.correr())
            nombre = f"evidencias/logs/{esc}_{'con' if protegido else 'sin'}_proteccion.txt"
            with open(nombre, "w", encoding="utf-8") as f:
                f.write("\n".join(sim.lineas) + "\n")
            print()

    print("=" * 110)
    print(f"{'Escenario':<24}{'Modo':<16}{'Estado final':<30}{'Cobrado':>11}{'Vendedor':>11}"
          f"{'Reembolso':>11}{'Custodia':>10}  Consistente")
    for r in resultados:
        print(f"{r['escenario']:<24}{r['modo']:<16}{r['estado_final']:<30}{r['cobrado']:>11,}"
              f"{r['pagado_vendedor']:>11,}{r['reembolsado']:>11,}{r['en_custodia']:>10,}  {r['dinero_consistente']}")
    with open("evidencias/resultados_fallos.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=resultados[0].keys())
        w.writeheader()
        w.writerows(resultados)


if __name__ == "__main__":
    import os
    os.makedirs("evidencias/logs", exist_ok=True)
    p = argparse.ArgumentParser()
    p.add_argument("--escenario", default="todos", choices=ESCENARIOS + ["todos"])
    p.add_argument("--sin-proteccion", action="store_true", help="solo el modo sin proteccion")
    p.add_argument("--con-proteccion", action="store_true", help="solo el modo con proteccion")
    a = p.parse_args()
    escs = ESCENARIOS if a.escenario == "todos" else [a.escenario]
    if a.sin_proteccion:
        modos = [False]
    elif a.con_proteccion:
        modos = [True]
    else:
        modos = [False, True]
    asyncio.run(main(escs, modos))
