import argparse
import asyncio
import hashlib
import hmac
import time
import uuid
from datetime import datetime

#Datos para cada transaccion
def nueva_transaccion(numero, valor=500000):
    return {
        "id": f"ESCROW-{numero:04d}",
        "valor": valor,
        "pago_confirmado": False,
        "producto_despachado": False,
        "entrega_confirmada": False,
        "dinero_liberado": False,
        "alerta_auditoria": False,
    }

def registrar(mensaje):
    print(f"[{datetime.now():%H:%M:%S}] {mensaje}")
    
#Reglas de negocio
def regla_liberacion(transaccion, sistema):
    #El dinero solo se libera si ya se confirmo el pago, despacho y entrega
    if (transaccion["pago_confirmado"]
            and transaccion["producto_despachado"]
            and transaccion["entrega_confirmada"]
            and not transaccion["dinero_liberado"]):
        transaccion["dinero_liberado"] = True
        sistema.metricas["liberaciones"] += 1
        if sistema.detallado:
            registrar(f"{transaccion['id']}: DINERO LIBERADO (${transaccion['valor']:,})")

def regla_auditoria(transaccion, sistema):
    #Si el monto supera un millon dejamos una sola alerta
    if transaccion["valor"] > 1_000_000 and not transaccion["alerta_auditoria"]:
        transaccion["alerta_auditoria"] = True
        sistema.metricas["alertas"] += 1
        registrar(f"{transaccion['id']}: ALERTA por monto alto (${transaccion['valor']:,})")

#Servico de ESCROW y validacion de eventos

#Indicamos quien puede enviar cada tipo de evento 
ORIGEN_PERMITIDO = {
    "SOLICITUD_PAGO": "COMPRADOR",
    "PAGO_APROBADO": "PASARELA",
    "PRODUCTO_DESPACHADO": "VENDEDOR",
    "ENTREGA_CONFIRMADA": "COMPRADOR",
}

class SistemaEscrow:
    def __init__(self, trabajadores=1, reglas_extra=None, detallado=False):
        self.cola = asyncio.Queue() 
        self.transacciones = {}
        self.disponible = asyncio.Event()
        self.disponible.set()  
        self.trabajadores = trabajadores
        self.tareas_consumidoras = []
        self.detallado = detallado

        #Estas claves son solo para simular la seguridad de los mensajes
        self.claves = {}
        self.eventos_vistos = set()  
        self.fallos_simulados = set()
        self.fallos_aplicados = set()
        self.max_reintentos = 3
        self.eventos_no_procesados = [] #Guardamos los mensajes que necesitan revision
        self.reglas = [regla_liberacion] + (reglas_extra or [])
        self.metricas = {
            "procesados": 0, "rechazados": 0, "duplicados": 0,
            "reintentos": 0, "liberaciones": 0, "alertas": 0,
            "max_cola": 0,
        }

    def registrar_transaccion(self, numero, valor=500000):
        transaccion = nueva_transaccion(numero, valor)
        self.transacciones[transaccion["id"]] = transaccion
        #Creamos una clave diferente para cada participante
        for rol in ("COMPRADOR", "PASARELA", "VENDEDOR"):
            self.claves[(transaccion["id"], rol)] = uuid.uuid4().hex.encode()
        return transaccion["id"]

    #Genera una firma para comprobar los datos del evento y su origen
    def firmar(self, evento):
        clave = self.claves[(evento["id"], evento["origen"])]
        mensaje = (f"{evento['evento_id']}|{evento['id']}|"
                   f"{evento['tipo']}|{evento['origen']}").encode()
        return hmac.new(clave, mensaje, hashlib.sha256).hexdigest()

    def crear_evento(self, identificador, tipo, origen, autentico=True):
        evento = {
            "evento_id": uuid.uuid4().hex,
            "id": identificador,
            "tipo": tipo,
            "origen": origen,
            "intentos": 0,
        }
        evento["firma"] = self.firmar(evento) if autentico else "FIRMA_FALSA"
        return evento

    def verificar_evento(self, evento):
        if (evento.get("id") not in self.transacciones
                or ORIGEN_PERMITIDO.get(evento.get("tipo")) != evento.get("origen")):
            return False
        try:
            firma_esperada = self.firmar(evento)
            return hmac.compare_digest(firma_esperada, evento.get("firma", ""))
        except (KeyError, TypeError):
            return False
        
    async def publicar(self, evento):
        #Dejamos el mensaje en la cola para que el servicio de ESCROW lo procese
        await self.cola.put(evento)
        self.metricas["max_cola"] = max(self.metricas["max_cola"], self.cola.qsize())
        
    async def procesar(self, evento):
        #Esta espera nos ayuda a ver como se acumulan los mensajes en la cola y se procesan en paralelo
        await asyncio.sleep(0.001)
        if not self.verificar_evento(evento):
            self.metricas["rechazados"] += 1
            registrar(f"{evento.get('id')}: evento RECHAZADO (firma u origen invalidos)")
            return

        #Revisamos si este mensaje ya se habia procesado
        if evento["evento_id"] in self.eventos_vistos:
            self.metricas["duplicados"] += 1
            registrar(f"{evento['id']}: evento duplicado IGNORADO")
            return

        #Para la prueba provocamos un error solo la primera vez que se procesa un evento de cierto tipo
        if (evento["tipo"] in self.fallos_simulados
                and evento["evento_id"] not in self.fallos_aplicados):
            self.fallos_aplicados.add(evento["evento_id"])
            raise RuntimeError("fallo temporal simulado")

        t = self.transacciones[evento["id"]]
        tipo = evento["tipo"]
        if tipo == "SOLICITUD_PAGO":
            pass #La solicitud inicia el proceso pero no cambia indicadores todavia
        elif tipo == "PAGO_APROBADO":
            t["pago_confirmado"] = True
        elif tipo == "PRODUCTO_DESPACHADO":
            t["producto_despachado"] = True
        elif tipo == "ENTREGA_CONFIRMADA":
            t["entrega_confirmada"] = True

        for regla in self.reglas:
            regla(t, self)

        self.eventos_vistos.add(evento["evento_id"])
        self.metricas["procesados"] += 1

    async def servicio_escrow(self):
        #Aqui atendemos los mensajes que van llegando
        while True:
            await self.disponible.wait() #Si el servicio esta pausado espera aqui
            evento = await self.cola.get()
            try:
                await self.procesar(evento)
            except asyncio.CancelledError:
                #Si se cancela la tarea intentamos devolver el mensaje a la cola 
                self.cola.put_nowait(evento)
                raise
            except Exception as error:
                #Si hay un error intentamos procesar el mensaje otra vez
                evento["intentos"] += 1
                if evento["intentos"] <= self.max_reintentos:
                    self.metricas["reintentos"] += 1
                    registrar(f"{evento['id']}: {error}; reintentando")
                    self.cola.put_nowait(evento)
                else:
                    self.eventos_no_procesados.append(evento)
                    registrar(f"{evento['id']}: requiere revision manual")
            finally:
                #Marcamos este intento como terminado incluso si fallo
                self.cola.task_done()

    def iniciar(self):
        for _ in range(self.trabajadores):
            self.tareas_consumidoras.append(asyncio.create_task(self.servicio_escrow()))

    async def finalizar(self):
        await self.cola.join()
        for tarea in self.tareas_consumidoras:
            tarea.cancel()
        await asyncio.gather(*self.tareas_consumidoras, return_exceptions=True)

    def resumen(self, duracion):
        total = len(self.transacciones)
        liberadas = sum(t["dinero_liberado"] for t in self.transacciones.values())
        print("\n--- RESULTADOS DE LA PRUEBA ---")
        print(f"Transacciones iniciadas: {total}")
        print(f"Transacciones con dinero liberado: {liberadas}")
        print(f"Eventos procesados validos: {self.metricas['procesados']}")
        print(f"Eventos rechazados: {self.metricas['rechazados']}")
        print(f"Eventos duplicados ignorados: {self.metricas['duplicados']}")
        print(f"Reintentos realizados: {self.metricas['reintentos']}")
        print(f"Eventos en revision manual: {len(self.eventos_no_procesados)}")
        print(f"Alertas por monto alto: {self.metricas['alertas']}")
        print(f"Maximo de mensajes en cola: {self.metricas['max_cola']}")
        print(f"Tiempo total medido: {duracion:.3f} segundos")
        if duracion > 0:
            print(f"Rendimiento observado: {total / duracion:.2f} transacciones/segundo")

#Participantes de la transaccion
async def comprador(sistema, identificador):
    await asyncio.sleep(0.002)
    await sistema.publicar(sistema.crear_evento(identificador, "SOLICITUD_PAGO", "COMPRADOR"))
    await asyncio.sleep(0.006)
    await sistema.publicar(sistema.crear_evento(identificador, "ENTREGA_CONFIRMADA", "COMPRADOR"))

async def pasarela_pago(sistema, identificador):
    await asyncio.sleep(0.004)
    await sistema.publicar(sistema.crear_evento(identificador, "PAGO_APROBADO", "PASARELA"))

async def vendedor(sistema, identificador):
    await asyncio.sleep(0.006)
    await sistema.publicar(sistema.crear_evento(identificador, "PRODUCTO_DESPACHADO", "VENDEDOR"))

async def ejecutar_flujo(sistema, ids):
    tareas = []
    for identificador in ids:
        tareas.extend([
            asyncio.create_task(comprador(sistema, identificador)),
            asyncio.create_task(pasarela_pago(sistema, identificador)),
            asyncio.create_task(vendedor(sistema, identificador)),
        ])
    await asyncio.gather(*tareas)

#Pruebas de los atributos de calidad

async def prueba_carga(nombre, cantidad, trabajadores):
    registrar(f"{nombre}: {cantidad} transacciones, {trabajadores} consumidor(es)")
    sistema = SistemaEscrow(trabajadores=trabajadores)
    ids = [sistema.registrar_transaccion(i) for i in range(1, cantidad + 1)]
    inicio = time.perf_counter()
    sistema.iniciar()
    await ejecutar_flujo(sistema, ids)
    await sistema.finalizar()
    sistema.resumen(time.perf_counter() - inicio)
    return sistema

async def prueba_disponibilidad():
    registrar("DISPONIBILIDAD: servicio ESCROW detenido temporalmente")
    sistema = SistemaEscrow()
    identificador = sistema.registrar_transaccion(1)
    sistema.disponible.clear() #Pausamos el servicio de ESCROW pero los mensajes siguen llegando a la cola
    sistema.iniciar()
    inicio = time.perf_counter()
    await ejecutar_flujo(sistema, [identificador])
    print(f"Mensajes pendientes mientras ESCROW esta detenido: {sistema.cola.qsize()}")
    print(f"Dinero liberado durante la pausa: {sistema.transacciones[identificador]['dinero_liberado']}")
    sistema.disponible.set() #Volvemos a habilitar el servicio de ESCROW para procesar los mensajes pendientes
    registrar("DISPONIBILIDAD: servicio reanudado")
    await sistema.finalizar()
    sistema.resumen(time.perf_counter() - inicio)
    return sistema

async def prueba_confiabilidad():
    registrar("CONFIABILIDAD: error temporal y mensaje duplicado")
    sistema = SistemaEscrow(detallado=True)
    identificador = sistema.registrar_transaccion(1)
    sistema.fallos_simulados.add("PAGO_APROBADO")
    sistema.iniciar()
    inicio = time.perf_counter()
    for tipo, origen in [
        ("SOLICITUD_PAGO", "COMPRADOR"),
        ("PAGO_APROBADO", "PASARELA"),
        ("PRODUCTO_DESPACHADO", "VENDEDOR"),
        ("ENTREGA_CONFIRMADA", "COMPRADOR"),
    ]:
        evento = sistema.crear_evento(identificador, tipo, origen)
        await sistema.publicar(evento)
        if tipo == "PAGO_APROBADO":
            await sistema.publicar(dict(evento)) #Enviamos otra copia del evento para probar los duplicados
    await sistema.finalizar()
    sistema.resumen(time.perf_counter() - inicio)
    return sistema

async def prueba_seguridad():
    registrar("SEGURIDAD: confirmar entrega con firma falsa")
    sistema = SistemaEscrow(detallado=True)
    identificador = sistema.registrar_transaccion(1)
    sistema.iniciar()
    inicio = time.perf_counter()
    for tipo, origen in [
        ("SOLICITUD_PAGO", "COMPRADOR"),
        ("PAGO_APROBADO", "PASARELA"),
        ("PRODUCTO_DESPACHADO", "VENDEDOR"),
    ]:
        await sistema.publicar(sistema.crear_evento(identificador, tipo, origen))
   #Probamos una confirmacion con una firma falsa
    evento_falso = sistema.crear_evento(identificador, "ENTREGA_CONFIRMADA", "COMPRADOR", autentico=False)
    await sistema.publicar(evento_falso)
    await sistema.cola.join()
    print(f"¿Se libero dinero tras el intento falso?: {sistema.transacciones[identificador]['dinero_liberado']}")
    #Despues enviamos una confirmacion valida para completar la transaccion
    await sistema.publicar(sistema.crear_evento(identificador, "ENTREGA_CONFIRMADA", "COMPRADOR"))
    await sistema.finalizar()
    sistema.resumen(time.perf_counter() - inicio)
    return sistema

async def prueba_mantenibilidad():
    registrar("MANTENIBILIDAD: agregar regla de auditoria sin tocar participantes")
    sistema = SistemaEscrow(reglas_extra=[regla_auditoria], detallado=True)
    identificador = sistema.registrar_transaccion(1, valor=1_500_000)
    inicio = time.perf_counter()
    sistema.iniciar()
    await ejecutar_flujo(sistema, [identificador])
    await sistema.finalizar()
    sistema.resumen(time.perf_counter() - inicio)
    print("Regla agregada: monto superior a $1.000.000 genera alerta.")
    return sistema

#Main para ejecutar las pruebas de calidad
async def main(actividad):
    if actividad in ("rendimiento", "todos"):
        print("\n=== 1. RENDIMIENTO ===")
        await prueba_carga("RENDIMIENTO", cantidad=100, trabajadores=1)
    if actividad in ("disponibilidad", "todos"):
        print("\n=== 2. DISPONIBILIDAD ===")
        await prueba_disponibilidad()
    if actividad in ("confiabilidad", "todos"):
        print("\n=== 3. CONFIABILIDAD ===")
        await prueba_confiabilidad()
    if actividad in ("seguridad", "todos"):
        print("\n=== 4. SEGURIDAD ===")
        await prueba_seguridad()
    if actividad in ("escalabilidad", "todos"):
        print("\n=== 5. ESCALABILIDAD ===")
        #Probamos las mismas 1000 transacciones con diferente cantidad de consumidores
        await prueba_carga("ESCALABILIDAD - 1 consumidor", cantidad=1000, trabajadores=1)
        await prueba_carga("ESCALABILIDAD - 4 consumidores", cantidad=1000, trabajadores=4)
    if actividad in ("mantenibilidad", "todos"):
        print("\n=== 6. MANTENIBILIDAD ===")
        await prueba_mantenibilidad()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Laboratorio 8 - Actividad 1 ESCROW")
    parser.add_argument(
        "prueba", nargs="?", default="rendimiento",
        choices=["rendimiento", "disponibilidad", "confiabilidad", "seguridad",
                 "escalabilidad", "mantenibilidad", "todos"],
        help="Atributo de calidad que desea comprobar",
    )
    args = parser.parse_args()
    asyncio.run(main(args.prueba))
