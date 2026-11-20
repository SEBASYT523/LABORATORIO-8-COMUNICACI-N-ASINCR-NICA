"""
ACTIVIDAD 5 - Pasarela de pago (proceso independiente).
Escucha solicitudes de cobro del ESCROW en 'pasarela.q' y responde con el
evento PAGO_APROBADO en el exchange de eventos.

Uso:  python pasarela.py
"""
import asyncio
import json
import random

from comun import Q_PASARELA, conectar, declarar_topologia, log, publicar


async def main():
    conexion = await conectar()
    canal = await conexion.channel()
    await canal.set_qos(prefetch_count=20)
    ex = await declarar_topologia(canal)
    cola = await canal.get_queue(Q_PASARELA)
    log("PASARELA", "Esperando solicitudes de cobro...")

    async def atender(msg):
        sol = json.loads(msg.body)
        await asyncio.sleep(random.uniform(0.5, 2.0))          # latencia del banco
        await publicar(ex["eventos"], "evento.PAGO_APROBADO",
                       {"tipo": "PAGO_APROBADO", "origen": "PASARELA", "transaccion_id": sol["transaccion_id"]})
        log("PASARELA", f"Pago aprobado {sol['transaccion_id']} (${sol['valor']:,})")
        await msg.ack()

    await cola.consume(lambda m: asyncio.create_task(atender(m)))
    await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
