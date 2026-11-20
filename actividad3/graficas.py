"""
Genera las evidencias graficas de la Actividad 3 a partir de los CSV que
produce escrow_multi.py.

    python escrow_multi.py
    python escrow_multi.py --n 1000 --costo-ms 5 --workers 1  --quiet --out evidencias/carga_1000tx_1consumidores
    python escrow_multi.py --n 1000 --costo-ms 5 --workers 4  --quiet --out evidencias/carga_1000tx_4consumidores
    python escrow_multi.py --n 1000 --costo-ms 5 --workers 16 --quiet --out evidencias/carga_1000tx_16consumidores
    python graficas.py
"""
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

TINTA = "#0b0b0b"
TINTA2 = "#52514e"
GRID = "#e4e3df"
FASES = [  # (estado inicial, estado final, etiqueta, color)
    ("INICIADA", "PAGO_RETENIDO", "Esperando pago (pasarela)", "#2a78d6"),
    ("PAGO_RETENIDO", "PRODUCTO_DESPACHADO", "Dinero retenido, vendedor empaca", "#eb6834"),
    ("PRODUCTO_DESPACHADO", "ENTREGA_CONFIRMADA", "En transporte / comprador revisa", "#1baf7a"),
]

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10,
    "axes.edgecolor": GRID, "axes.labelcolor": TINTA2, "xtick.color": TINTA2,
    "ytick.color": TINTA2, "axes.titlecolor": TINTA, "axes.titleweight": "bold",
    "axes.spines.top": False, "axes.spines.right": False,
})


def leer(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------- 1. Linea de tiempo (Gantt) de las 10 transacciones ----------
txs = leer("evidencias/10tx_transacciones.csv")
fig, ax = plt.subplots(figsize=(10, 5.2))
for i, tx in enumerate(reversed(txs)):
    for ini, fin, etiqueta, color in FASES:
        a, b = float(tx[ini]), float(tx[fin])
        ax.barh(i, b - a, left=a, height=0.62, color=color, edgecolor="white", linewidth=2,
                label=etiqueta if i == 0 else None)
    lib = float(tx["DINERO_LIBERADO"])
    ax.plot(lib, i, marker="D", color=TINTA, markersize=6, label="Dinero liberado" if i == 0 else None)
    ax.text(lib + 0.08, i, f"${int(tx['valor']):,}".replace(",", "."), va="center", fontsize=8.5, color=TINTA2)
ax.set_yticks(range(len(txs)))
ax.set_yticklabels([f"{t['id']}  ({t['comprador'][-2:]}↔{t['vendedor'][-2:]})" for t in reversed(txs)])
ax.set_xlabel("Tiempo desde el inicio de la simulación (s)")
ax.set_title("10 transacciones ESCROW ejecutándose en paralelo", loc="left")
ax.set_xlim(0, max(float(t["DINERO_LIBERADO"]) for t in txs) + 1.1)
ax.grid(axis="x", color=GRID)
ax.set_axisbelow(True)
ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=4, frameon=False, fontsize=8.5)
fig.tight_layout()
fig.savefig("evidencias/fig1_linea_tiempo_10tx.png", dpi=160)

# ---------- 2. Eventos en la cola y entrelazado de eventos (10 tx) ----------
bit = leer("evidencias/10tx_bitacora.csv")
proc = [r for r in bit if r["accion"].startswith("PROCESA_")]
ids = sorted({r["transaccion"] for r in proc})
fig, ax = plt.subplots(figsize=(10, 3.6))
y = [ids.index(r["transaccion"]) + 1 for r in proc]
ax.plot(range(1, len(proc) + 1), y, color="#2a78d6", linewidth=1.2, alpha=0.5)
ax.scatter(range(1, len(proc) + 1), y, s=34, color="#2a78d6", edgecolor="white", linewidth=1.5, zorder=3)
ax.set_yticks(range(1, len(ids) + 1))
ax.set_yticklabels([i[-3:] for i in ids], fontsize=8)
ax.set_xlabel("Orden en que el ESCROW consumió el evento (1 … 40)")
ax.set_ylabel("Transacción")
ax.set_title("Orden de consumo: los eventos de distintas transacciones se intercalan en la cola", loc="left")
ax.grid(color=GRID)
ax.set_axisbelow(True)
fig.tight_layout()
fig.savefig("evidencias/fig2_intercalado_eventos.png", dpi=160)

# ---------- 3. Tamaño de la cola en el tiempo: 1000 tx, 1 vs 4 vs 16 consumidores ----------
fig, ax = plt.subplots(figsize=(10, 4))
for w, color in [(1, "#2a78d6"), (4, "#eb6834"), (16, "#1baf7a")]:
    rows = leer(f"evidencias/carga_1000tx_{w}consumidores_bitacora.csv")
    t = [float(r["t"]) for r in rows]
    q = [int(r["tam_cola"]) for r in rows]
    ax.plot(t, q, color=color, linewidth=2,
            label=f"{w} consumidor{'es' if w > 1 else ''} ESCROW (termina en {t[-1]:.1f} s)")
ax.set_xlabel("Tiempo (s)")
ax.set_ylabel("Eventos esperando en la cola")
ax.set_title("1.000 transacciones, 5 ms por evento: la cola crece si hay un solo consumidor", loc="left")
ax.grid(color=GRID)
ax.set_axisbelow(True)
ax.legend(frameon=False)
fig.tight_layout()
fig.savefig("evidencias/fig3_cola_1000tx_consumidores.png", dpi=160)
print("Graficas generadas en evidencias/: fig1_linea_tiempo_10tx.png, fig2_intercalado_eventos.png, fig3_cola_1000tx_consumidores.png")
