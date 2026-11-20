"""
ACTIVIDAD 5 - Compradores (proceso independiente).
Inician N transacciones con valores diferentes y confirman la recepcion
cuando la transportadora les entrega el producto.

Uso:
    python comprador.py --n 10
    python comprador.py --n 10 --duplicar     # reenvia cada confirmacion (mismo evento_id)
    python comprador.py --n 3  --intruso      # ademas, un intruso confirma una entrega sin despacho
"""
import argparse
import asyncio
import json
import random
import uuid

from comun import Q_COMPRADOR, conectar, declarar_topologia, log, publicar


async def main(n, duplicar, intruso):
    conexion = await conectar()
    canal = await conexion.channel()
    await canal.set_qos(prefetch_count=20)
    ex = await declarar_topologia(canal)
    cola = await canal.get_queue(Q_COMPRADOR)
    lote = uuid.uuid4().hex[:4].upper()
    pendientes = set()

    async def atender(msg):
        d = json.loads(msg.body)
        tx = d["transaccion_id"]
        if msg.routing_key == "logistica.entregado" and tx in pendientes:
            await asyncio.sleep(random.uniform(0.2, 1.5))     # revisa el producto
            ev_id = await publicar(ex["eventos"], "evento.ENTREGA_CONFIRMADA",
                                   {"tipo": "ENTREGA_CONFIRMADA", "origen": "COMPRADOR", "transaccion_id": tx})
            log("COMPRADOR", f"Producto de {tx} recibido -> confirma entrega")
            if duplicar:
                await publicar(ex["eventos"], "evento.ENTREGA_CONFIRMADA",
                               {"tipo": "ENTREGA_CONFIRMADA", "origen": "COMPRADOR", "transaccion_id": tx},
                               evento_id=ev_id)
                log("COMPRADOR", f"(reenvio duplicado de la confirmacion de {tx}, evento_id={ev_id})")
            pendientes.discard(tx)
        await msg.ack()

    await cola.consume(lambda m: asyncio.create_task(atender(m)))

    for i in range(1, n + 1):
        tx = f"TX-{lote}-{i:03d}"
        valor = random.randrange(50_000, 3_000_001, 10_000)
        pendientes.add(tx)
        await publicar(ex["eventos"], "evento.SOLICITUD_PAGO",
                       {"tipo": "SOLICITUD_PAGO", "origen": f"COMPRADOR-{i:02d}", "transaccion_id": tx,
                        "comprador": f"COMPRADOR-{i:02d}", "vendedor": f"VENDEDOR-{i:02d}", "valor": valor})
        log("COMPRADOR", f"COMPRADOR-{i:02d} inicia {tx} por ${valor:,}")

    if intruso:
        await asyncio.sleep(0.3)
        victima = f"TX-{lote}-001"
        await publicar(ex["eventos"], "evento.ENTREGA_CONFIRMADA",
                       {"tipo": "ENTREGA_CONFIRMADA", "origen": "INTRUSO", "transaccion_id": victima})
        log("INTRUSO", f"Envia ENTREGA_CONFIRMADA falsa para {victima} (aun no despachada)")

    while pendientes:
        await asyncio.sleep(0.2)
    await asyncio.sleep(0.5)
    log("COMPRADOR", f"Todas las {n} entregas confirmadas")
    await conexion.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=10)
    p.add_argument("--duplicar", action="store_true")
    p.add_argument("--intruso", action="store_true")
    a = p.parse_args()
    asyncio.run(main(a.n, a.duplicar, a.intruso))
