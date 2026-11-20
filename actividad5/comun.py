"""
Laboratorio 8 - ACTIVIDAD 5: Mensajeria distribuida con RabbitMQ.
Modulo comun: conexion, topologia (exchanges/colas) y utilidades.

Topologia en RabbitMQ
---------------------
  escrow.eventos (topic, durable)
      participantes publican  evento.<TIPO>        ->  cola escrow.eventos.q
                                                       (durable, consumida por el ESCROW,
                                                        con dead-letter hacia escrow.dlx)
  escrow.notificaciones (topic, durable)
      ESCROW publica  pasarela.cobro              ->  cola pasarela.q
                      vendedor.pago_retenido      ->  cola vendedor.q
                      comprador.<algo>            ->  cola comprador.q
      vendedor publica logistica.entregado        ->  cola comprador.q  (simula la transportadora)
  escrow.dlx (fanout, durable)                    ->  cola escrow.eventos.dlq (mensajes muertos)

Todos los mensajes son persistentes (delivery_mode=PERSISTENT) y llevan
message_id = evento_id, que el ESCROW usa para garantizar idempotencia.
"""
import json
import os
import uuid
from datetime import datetime

import aio_pika

URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

EX_EVENTOS = "escrow.eventos"
EX_NOTIF = "escrow.notificaciones"
EX_DLX = "escrow.dlx"
Q_ESCROW = "escrow.eventos.q"
Q_DLQ = "escrow.eventos.dlq"
Q_PASARELA = "pasarela.q"
Q_VENDEDOR = "vendedor.q"
Q_COMPRADOR = "comprador.q"


def log(actor, msg):
    print(f"[{datetime.now():%H:%M:%S.%f}"[:-3] + f"] {actor:<10} {msg}", flush=True)


async def conectar():
    """Conexion robusta: reintenta y se reconecta sola si RabbitMQ se cae."""
    return await aio_pika.connect_robust(URL)


async def declarar_topologia(canal):
    ex_eventos = await canal.declare_exchange(EX_EVENTOS, aio_pika.ExchangeType.TOPIC, durable=True)
    ex_notif = await canal.declare_exchange(EX_NOTIF, aio_pika.ExchangeType.TOPIC, durable=True)
    ex_dlx = await canal.declare_exchange(EX_DLX, aio_pika.ExchangeType.FANOUT, durable=True)

    q_dlq = await canal.declare_queue(Q_DLQ, durable=True)
    await q_dlq.bind(ex_dlx)

    q_escrow = await canal.declare_queue(Q_ESCROW, durable=True,
                                         arguments={"x-dead-letter-exchange": EX_DLX})
    await q_escrow.bind(ex_eventos, routing_key="evento.#")

    q_pas = await canal.declare_queue(Q_PASARELA, durable=True)
    await q_pas.bind(ex_notif, routing_key="pasarela.#")
    q_ven = await canal.declare_queue(Q_VENDEDOR, durable=True)
    await q_ven.bind(ex_notif, routing_key="vendedor.#")
    q_com = await canal.declare_queue(Q_COMPRADOR, durable=True)
    await q_com.bind(ex_notif, routing_key="comprador.#")
    await q_com.bind(ex_notif, routing_key="logistica.#")

    return {"eventos": ex_eventos, "notif": ex_notif, "dlx": ex_dlx}


async def publicar(exchange, routing_key, cuerpo, evento_id=None):
    evento_id = evento_id or uuid.uuid4().hex[:10]
    cuerpo = {**cuerpo, "evento_id": evento_id}
    await exchange.publish(
        aio_pika.Message(
            body=json.dumps(cuerpo).encode(),
            message_id=evento_id,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,   # sobrevive a reinicios del broker
        ),
        routing_key=routing_key,
    )
    return evento_id
