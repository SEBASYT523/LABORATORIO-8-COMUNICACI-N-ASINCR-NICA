"""
ACTIVIDAD 5 - Vendedores (proceso independiente).
Reciben la notificacion de pago retenido, despachan el producto y la
transportadora (simulada) avisa al comprador cuando el producto llega.

Uso:  python vendedor.py
"""
import asyncio
import json
import random

from comun import Q_VENDEDOR, conectar, declarar_topologia, log, publicar


async def main():
    conexion = await conectar()
    canal = await conexion.channel()
    await canal.set_qos(prefetch_count=20)
    ex = await declarar_topologia(canal)
    cola = await canal.get_queue(Q_VENDEDOR)
    log("VENDEDOR", "Esperando notificaciones del ESCROW...")

    async def atender(msg):
        n = json.loads(msg.body)
        tx = n["transaccion_id"]
        if msg.routing_key == "vendedor.pago_retenido":
            log("VENDEDOR", f"{n['vendedor']}: pago de {tx} retenido, preparando despacho")
            await asyncio.sleep(random.uniform(0.3, 1.5))
            await publicar(ex["eventos"], "evento.PRODUCTO_DESPACHADO",
                           {"tipo": "PRODUCTO_DESPACHADO", "origen": n["vendedor"], "transaccion_id": tx})
            await msg.ack()
            await asyncio.sleep(random.uniform(0.5, 2.0))       # transporte
            await publicar(ex["notif"], "logistica.entregado", {"transaccion_id": tx})
            log("LOGISTICA", f"Producto de {tx} entregado al comprador")
        elif msg.routing_key == "vendedor.dinero_liberado":
            log("VENDEDOR", f"{n['vendedor']}: recibio ${n['valor']:,} por {tx}")
            await msg.ack()
        else:
            await msg.ack()

    await cola.consume(lambda m: asyncio.create_task(atender(m)))
    await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
