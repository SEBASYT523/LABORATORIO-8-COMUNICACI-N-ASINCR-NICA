"""Grafica de la prueba de disponibilidad (evidencias/B_monitor.csv)."""
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

rows = list(csv.DictReader(open("evidencias/B_monitor.csv")))
t = [float(r["t"]) for r in rows]
listos = [int(r["escrow_listos"]) for r in rows]
cons = [int(r["escrow_consumidores"]) for r in rows]
dlq = [int(r["dlq"]) for r in rows]

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.edgecolor": "#e4e3df",
                     "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.labelcolor": "#52514e"})
fig, ax = plt.subplots(figsize=(10, 4))
caida = [ti for ti, c in zip(t, cons) if c == 0]
ax.axvspan(min(caida), max(caida), color="#e4e3df", alpha=0.6, lw=0)
ax.text((min(caida) + max(caida)) / 2, 12.3, "Servicio ESCROW apagado", ha="center", color="#52514e", fontsize=9)
ax.step(t, listos, where="post", color="#2a78d6", lw=2, label="Mensajes esperando en escrow.eventos.q")
ax.step(t, dlq, where="post", color="#eb6834", lw=2, label="Mensajes en la cola de mensajes muertos (DLQ)")
ax.set_ylim(0, 13.5)
ax.set_xlabel("Tiempo (s)")
ax.set_ylabel("Mensajes")
ax.set_title("RabbitMQ retiene los eventos mientras el ESCROW está caído y los entrega al volver",
             loc="left", fontweight="bold", color="#0b0b0b")
ax.grid(color="#e4e3df")
ax.set_axisbelow(True)
ax.legend(frameon=False, loc="center right")
fig.tight_layout()
fig.savefig("evidencias/fig5_disponibilidad_rabbitmq.png", dpi=160)
print("evidencias/fig5_disponibilidad_rabbitmq.png")
