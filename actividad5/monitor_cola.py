"""
Mide en tiempo real (cada 0.2 s) cuantos mensajes esperan en la cola del
ESCROW y en la DLQ, y cuantos consumidores tiene la cola. Usa una declaracion
pasiva por AMQP (la consola web de RabbitMQ solo actualiza cada 5 s).

Uso:  python monitor_cola.py --segundos 25 --out evidencias/B_monitor.csv
"""
import argparse
import asyncio
import csv
import time

from comun import Q_DLQ, Q_ESCROW, conectar


async def main(segundos, salida):
    conexion = await conectar()
    canal = await conexion.channel()
    t0 = time.time()
    with open(salida, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t", "escrow_listos", "escrow_consumidores", "dlq"])
        while time.time() - t0 < segundos:
            bajo = await canal.get_underlay_channel()          # canal AMQP sin cache
            q = await bajo.queue_declare(Q_ESCROW, passive=True)
            d = await bajo.queue_declare(Q_DLQ, passive=True)
            w.writerow([round(time.time() - t0, 2), q.message_count, q.consumer_count, d.message_count])
            f.flush()
            await asyncio.sleep(0.2)
    await conexion.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--segundos", type=float, default=25)
    p.add_argument("--out", default="monitor.csv")
    a = p.parse_args()
    asyncio.run(main(a.segundos, a.out))
