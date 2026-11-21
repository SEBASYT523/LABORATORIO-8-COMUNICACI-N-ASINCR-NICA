import argparse
import asyncio
import hashlib
import hmac
import uuid
from datetime import datetime


#Datos de cada transaccion

def nueva_transaccion(numero, valor=500_000):
    return {
        "id": f"ESCROW-{numero:04d}",
        "valor": valor,
        "solicitud_registrada": False,
        "pago_confirmado": False,
        "producto_despachado": False,
        "entrega_confirmada": False,
        "dinero_liberado": False,
        "cancelada": False,
        "dinero_reembolsado": False,
        "saldo_custodia": 0,
        "saldo_vendedor": 0,
        "monto_reembolsado": 0,
    }

def registrar(mensaje):
    print(f"[{datetime.now():%H:%M:%S}] {mensaje}")


#La liberacion sigue siendo una regla independiente
def regla_liberacion(t, sistema):
    if (t["pago_confirmado"] and t["producto_despachado"]
            and t["entrega_confirmada"] and not t["cancelada"]
            and not t["dinero_reembolsado"] and not t["dinero_liberado"]
            and t["saldo_custodia"] >= t["valor"]):
        # El valor sale de custodia y pasa al vendedor una sola vez
        t["saldo_custodia"] -= t["valor"]
        t["saldo_vendedor"] += t["valor"]
        t["dinero_liberado"] = True
        sistema.metricas["liberaciones"] += 1
        registrar(f"{t['id']}: DINERO LIBERADO (${t['valor']:,})")

#Mensajes que puede enviar cada participante
ORIGEN_PERMITIDO = {
    "SOLICITUD_PAGO": "COMPRADOR",
    "PAGO_APROBADO": "PASARELA",
    "PRODUCTO_DESPACHADO": "VENDEDOR",
    "ENTREGA_CONFIRMADA": "COMPRADOR",
    "CANCELAR_TRANSACCION": "COMPRADOR",
    "REEMBOLSO_SOLICITADO": "COMPRADOR",
}

class SistemaEscrow:
    def __init__(self, reglas_extra=None):
        self.cola = asyncio.Queue()
        self.transacciones = {}
        self.claves = {}  #Claves de prueba
        self.eventos_vistos = set()
        self.max_reintentos = 3
        self.eventos_no_procesados = []
        self.reglas = [regla_liberacion] + (reglas_extra or [])
        self.tarea_consumidora = None
        self.metricas = {
            "procesados": 0,
            "rechazados": 0,
            "rechazos_negocio": 0,
            "duplicados": 0,
            "reintentos": 0,
            "cancelaciones": 0,
            "reembolsos": 0,
            "liberaciones": 0,
        }

    def registrar_transaccion(self, numero, valor=500_000):
        t = nueva_transaccion(numero, valor)
        self.transacciones[t["id"]] = t
        for rol in ("COMPRADOR", "PASARELA", "VENDEDOR"):
            self.claves[(t["id"], rol)] = uuid.uuid4().hex.encode()
        return t["id"]

    def firmar(self, evento):
        """Firma el mensaje usando la clave simulada de su participante."""
        clave = self.claves[(evento["id"], evento["origen"])]
        mensaje = (f"{evento['evento_id']}|{evento['id']}|"
                   f"{evento['tipo']}|{evento['origen']}").encode()
        return hmac.new(clave, mensaje, hashlib.sha256).hexdigest()

    def crear_evento(self, identificador, tipo, origen):
        evento = {
            "evento_id": uuid.uuid4().hex,
            "id": identificador,
            "tipo": tipo,
            "origen": origen,
            "intentos": 0,
        }
        evento["firma"] = self.firmar(evento)
        return evento

    def verificar_evento(self, evento):
        if (evento.get("id") not in self.transacciones
                or ORIGEN_PERMITIDO.get(evento.get("tipo")) != evento.get("origen")):
            return False
        try:
            return hmac.compare_digest(self.firmar(evento), evento.get("firma", ""))
        except (KeyError, TypeError):
            return False

    def comprobar_operacion(self, t, tipo):
        """Revisa si la operación tiene sentido en el estado actual."""
        if tipo == "SOLICITUD_PAGO":
            if t["solicitud_registrada"] or t["cancelada"]:
                return "la solicitud ya existe o la compra está cancelada"
        elif tipo == "PAGO_APROBADO":
            if t["cancelada"]:
                return "la compra ya está cancelada"
            if not t["solicitud_registrada"] or t["pago_confirmado"]:
                return "no se solicitó el pago o ya estaba aprobado"
        elif tipo == "PRODUCTO_DESPACHADO":
            if t["cancelada"] or not t["pago_confirmado"] or t["producto_despachado"]:
                return "no hay pago retenido, está cancelada o ya se despachó"
        elif tipo == "ENTREGA_CONFIRMADA":
            #Una firma valida no reemplaza la constancia de despacho
            if not t["producto_despachado"]:
                return "no hay despacho registrado"
            if t["cancelada"] or t["entrega_confirmada"] or not t["pago_confirmado"]:
                return "la entrega ya se confirmó o la compra no está activa"
        elif tipo == "CANCELAR_TRANSACCION":
            #Adoptamos una politica sencilla, se puede cancelar antes del despacho
            if not t["solicitud_registrada"]:
                return "primero debe iniciarse la compra"
            if t["cancelada"] or t["producto_despachado"] or t["dinero_liberado"]:
                return "la compra ya se canceló, despachó o se liberó el dinero"
        elif tipo == "REEMBOLSO_SOLICITADO":
            if not t["cancelada"]:
                return "primero debe cancelarse la compra"
            if not t["pago_confirmado"] or t["dinero_reembolsado"]:
                return "no hay pago retenido o el reembolso ya se realizó"
            if t["saldo_custodia"] < t["valor"]:
                return "no hay saldo suficiente en custodia"
        return None

    def estado(self, identificador):
        t = self.transacciones[identificador]
        if t["dinero_reembolsado"]:
            return "REEMBOLSADA"
        if t["cancelada"]:
            return "CANCELADA"
        if t["dinero_liberado"]:
            return "DINERO_LIBERADO"
        if t["entrega_confirmada"]:
            return "ENTREGA_CONFIRMADA"
        if t["producto_despachado"]:
            return "PRODUCTO_DESPACHADO"
        if t["pago_confirmado"]:
            return "PAGO_RETENIDO"
        return "INICIADA"

    async def publicar(self, evento):
        await self.cola.put(evento)

    async def procesar(self, evento):
        if not self.verificar_evento(evento):
            self.metricas["rechazados"] += 1
            registrar(f"{evento.get('id')}: evento RECHAZADO (origen o firma inválidos)")
            return

        if evento["evento_id"] in self.eventos_vistos:
            self.metricas["duplicados"] += 1
            registrar(f"{evento['id']}: evento duplicado IGNORADO")
            return

        t = self.transacciones[evento["id"]]
        tipo = evento["tipo"]
        motivo = self.comprobar_operacion(t, tipo)
        if motivo:
            #Un evento invalido no cambia el estado ni el dinero
            self.metricas["rechazos_negocio"] += 1
            self.metricas["rechazados"] += 1
            self.eventos_vistos.add(evento["evento_id"])
            registrar(f"{t['id']}: {tipo} RECHAZADO ({motivo})")
            return

        if tipo == "SOLICITUD_PAGO":
            t["solicitud_registrada"] = True
        elif tipo == "PAGO_APROBADO":
            t["pago_confirmado"] = True
            t["saldo_custodia"] += t["valor"]
        elif tipo == "PRODUCTO_DESPACHADO":
            t["producto_despachado"] = True
        elif tipo == "ENTREGA_CONFIRMADA":
            t["entrega_confirmada"] = True
        elif tipo == "CANCELAR_TRANSACCION":
            t["cancelada"] = True
            self.metricas["cancelaciones"] += 1
            registrar(f"{t['id']}: transacción CANCELADA")
        elif tipo == "REEMBOLSO_SOLICITADO":
            t["saldo_custodia"] -= t["valor"]
            t["monto_reembolsado"] += t["valor"]
            t["dinero_reembolsado"] = True
            self.metricas["reembolsos"] += 1
            registrar(f"{t['id']}: REEMBOLSO al comprador (${t['valor']:,})")

        for regla in self.reglas:
            regla(t, self)
        self.eventos_vistos.add(evento["evento_id"])
        self.metricas["procesados"] += 1

    async def servicio_escrow(self):
        while True:
            evento = await self.cola.get()
            try:
                await self.procesar(evento)
            except asyncio.CancelledError:
                self.cola.put_nowait(evento)
                raise
            except Exception as error:
                evento["intentos"] += 1
                if evento["intentos"] <= self.max_reintentos:
                    self.metricas["reintentos"] += 1
                    registrar(f"{evento.get('id')}: {error}; reintentando")
                    self.cola.put_nowait(evento)
                else:
                    self.eventos_no_procesados.append(evento)
                    registrar(f"{evento.get('id')}: requiere revisión manual")
            finally:
                self.cola.task_done()

    def iniciar(self):
        self.tarea_consumidora = asyncio.create_task(self.servicio_escrow())

    async def finalizar(self):
        await self.cola.join()
        if self.tarea_consumidora:
            self.tarea_consumidora.cancel()
            await asyncio.gather(self.tarea_consumidora, return_exceptions=True)

    def resumen(self):
        print("\n--- RESUMEN DE LA ACTIVIDAD 2 ---")
        print(f"Eventos procesados: {self.metricas['procesados']}")
        print(f"Eventos rechazados: {self.metricas['rechazados']}")
        print(f"Rechazos por reglas de negocio: {self.metricas['rechazos_negocio']}")
        print(f"Cancelaciones registradas: {self.metricas['cancelaciones']}")
        print(f"Reembolsos realizados: {self.metricas['reembolsos']}")
        print(f"Liberaciones registradas: {self.metricas['liberaciones']}")

#Casos que usamos para comprobar los cambios
async def enviar_y_esperar(sistema, identificador, tipo, origen):
    await sistema.publicar(sistema.crear_evento(identificador, tipo, origen))
    await sistema.cola.join()

def detalle_transaccion(sistema, identificador):
    t = sistema.transacciones[identificador]
    print(f"\nTransacción: {identificador} | Estado final: {sistema.estado(identificador)}")
    print(f"Valor de la compra: ${t['valor']:,}")
    print(f"Dinero en custodia: ${t['saldo_custodia']:,}")
    print(f"Dinero al vendedor: ${t['saldo_vendedor']:,}")
    print(f"Dinero reembolsado: ${t['monto_reembolsado']:,}")

async def prueba_cancelacion():
    print("\n=== 2.1. CANCELACIÓN SIN PAGO ===")
    sistema = SistemaEscrow()
    identificador = sistema.registrar_transaccion(1, 500_000)
    sistema.iniciar()
    await enviar_y_esperar(sistema, identificador, "SOLICITUD_PAGO", "COMPRADOR")
    await enviar_y_esperar(sistema, identificador, "CANCELAR_TRANSACCION", "COMPRADOR")
    #Un pago aprobado despues no debe reabrir una compra cancelada
    await enviar_y_esperar(sistema, identificador, "PAGO_APROBADO", "PASARELA")
    await sistema.finalizar()
    detalle_transaccion(sistema, identificador)
    sistema.resumen()
    t = sistema.transacciones[identificador]
    assert t["cancelada"] and not t["dinero_liberado"]
    assert t["saldo_custodia"] == 0 and t["monto_reembolsado"] == 0
    assert sistema.metricas["rechazos_negocio"] == 1
    print("COMPROBACIÓN: OK - cancelación sin liberar dinero ni aceptar pago tardío")
    return sistema

async def prueba_reembolso():
    print("\n=== 2.2. CANCELACIÓN CON REEMBOLSO ===")
    sistema = SistemaEscrow()
    identificador = sistema.registrar_transaccion(1, 750_000)
    sistema.iniciar()
    await enviar_y_esperar(sistema, identificador, "SOLICITUD_PAGO", "COMPRADOR")
    await enviar_y_esperar(sistema, identificador, "PAGO_APROBADO", "PASARELA")
    print(f"Dinero retenido antes de cancelar: ${sistema.transacciones[identificador]['saldo_custodia']:,}")
    await enviar_y_esperar(sistema, identificador, "CANCELAR_TRANSACCION", "COMPRADOR")
    await enviar_y_esperar(sistema, identificador, "REEMBOLSO_SOLICITADO", "COMPRADOR")
    #Otra solicitud nueva no debe devolver el mismo dinero dos veces
    await enviar_y_esperar(sistema, identificador, "REEMBOLSO_SOLICITADO", "COMPRADOR")
    await sistema.finalizar()
    detalle_transaccion(sistema, identificador)
    sistema.resumen()
    t = sistema.transacciones[identificador]
    assert t["dinero_reembolsado"] and t["monto_reembolsado"] == 750_000
    assert t["saldo_custodia"] == 0 and t["saldo_vendedor"] == 0
    assert not t["dinero_liberado"] and sistema.metricas["reembolsos"] == 1
    assert sistema.metricas["rechazos_negocio"] == 1
    print("COMPROBACIÓN: OK - reembolso una sola vez y custodia en cero")
    return sistema

async def prueba_entrega_sin_despacho():
    print("\n=== 2.3. ENTREGA SIN DESPACHO ===")
    sistema = SistemaEscrow()
    identificador = sistema.registrar_transaccion(1, 500_000)
    sistema.iniciar()
    await enviar_y_esperar(sistema, identificador, "SOLICITUD_PAGO", "COMPRADOR")
    await enviar_y_esperar(sistema, identificador, "PAGO_APROBADO", "PASARELA")
    #La firma es valida pero todavia no hay despacho registrado
    await enviar_y_esperar(sistema, identificador, "ENTREGA_CONFIRMADA", "COMPRADOR")
    t = sistema.transacciones[identificador]
    print(f"Antes del despacho: entrega={t['entrega_confirmada']}, liberado={t['dinero_liberado']}")
    assert not t["entrega_confirmada"] and not t["dinero_liberado"]
    await enviar_y_esperar(sistema, identificador, "PRODUCTO_DESPACHADO", "VENDEDOR")
    await enviar_y_esperar(sistema, identificador, "ENTREGA_CONFIRMADA", "COMPRADOR")
    await sistema.finalizar()
    detalle_transaccion(sistema, identificador)
    sistema.resumen()
    assert t["entrega_confirmada"] and t["dinero_liberado"]
    assert t["saldo_vendedor"] == 500_000 and sistema.metricas["rechazos_negocio"] == 1
    print("COMPROBACIÓN: OK - entrega rechazada sin despacho y aceptada después")
    return sistema

async def prueba_flujo_normal():
    print("\n=== 2.4. FLUJO NORMAL ===")
    sistema = SistemaEscrow()
    identificador = sistema.registrar_transaccion(1, 500_000)
    sistema.iniciar()
    for tipo, origen in (
        ("SOLICITUD_PAGO", "COMPRADOR"),
        ("PAGO_APROBADO", "PASARELA"),
        ("PRODUCTO_DESPACHADO", "VENDEDOR"),
        ("ENTREGA_CONFIRMADA", "COMPRADOR"),
    ):
        await enviar_y_esperar(sistema, identificador, tipo, origen)
    await sistema.finalizar()
    detalle_transaccion(sistema, identificador)
    sistema.resumen()
    t = sistema.transacciones[identificador]
    assert t["dinero_liberado"] and t["saldo_vendedor"] == 500_000
    assert t["saldo_custodia"] == 0 and t["monto_reembolsado"] == 0
    assert sistema.metricas["rechazos_negocio"] == 0
    print("COMPROBACIÓN: OK - flujo normal conservado")
    return sistema

async def prueba_validaciones():
    print("\n=== 2.5. VALIDACIONES ADICIONALES ===")
    sistema = SistemaEscrow()
    identificador = sistema.registrar_transaccion(1, 500_000)
    sistema.iniciar()
    await enviar_y_esperar(sistema, identificador, "SOLICITUD_PAGO", "COMPRADOR")
    await enviar_y_esperar(sistema, identificador, "PAGO_APROBADO", "PASARELA")
    await enviar_y_esperar(sistema, identificador, "REEMBOLSO_SOLICITADO", "COMPRADOR")
    await enviar_y_esperar(sistema, identificador, "PRODUCTO_DESPACHADO", "VENDEDOR")
    await enviar_y_esperar(sistema, identificador, "CANCELAR_TRANSACCION", "COMPRADOR")
    await enviar_y_esperar(sistema, identificador, "ENTREGA_CONFIRMADA", "COMPRADOR")
    await sistema.finalizar()
    detalle_transaccion(sistema, identificador)
    sistema.resumen()
    t = sistema.transacciones[identificador]
    assert not t["cancelada"] and not t["dinero_reembolsado"] and t["dinero_liberado"]
    assert sistema.metricas["rechazos_negocio"] == 2
    print("COMPROBACIÓN: OK - rechazo de reembolso sin cancelar y cancelación tras despacho")
    return sistema

async def main(prueba):
    escenarios = {
        "cancelacion": prueba_cancelacion,
        "reembolso": prueba_reembolso,
        "sin_despacho": prueba_entrega_sin_despacho,
        "flujo_normal": prueba_flujo_normal,
        "validaciones": prueba_validaciones,
    }
    if prueba == "todos":
        for ejecutar in escenarios.values():
            await ejecutar()
    else:
        await escenarios[prueba]()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Laboratorio 8 - Actividad 2 ESCROW")
    parser.add_argument(
        "prueba", nargs="?", default="todos",
        choices=["todos", "cancelacion", "reembolso", "sin_despacho",
                 "flujo_normal", "validaciones"],
        help="Escenario de la actividad 2 que queremos ejecutar",
    )
    asyncio.run(main(parser.parse_args().prueba))
