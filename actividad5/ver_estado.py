"""
Muestra el estado de las transacciones guardado en PostgreSQL (Neon o local).

Uso:
    python ver_estado.py                  # todas las transacciones
    python ver_estado.py --historial TX-AB12-001
    python ver_estado.py --borrar         # vacia las tablas (para empezar una prueba limpia)
"""
import argparse
import asyncio

from bd import crear_pool, destino


async def main(historial, borrar):
    pool = await crear_pool()
    if borrar:
        await pool.execute("TRUNCATE historial, eventos_procesados, transacciones")
        print(f"Tablas vaciadas en {destino()}")
        return
    print(f"Base de datos: {destino()}\n")
    if historial:
        for r in await pool.fetch("SELECT momento, estado_anterior, estado_nuevo, evento_id FROM historial "
                                  "WHERE transaccion_id=$1 ORDER BY id", historial):
            print(f"{r['momento'].astimezone():%H:%M:%S.%f}  {r['estado_anterior']:<20} -> {r['estado_nuevo']:<20} [{r['evento_id']}]")
        return
    print(f"{'Transaccion':<16}{'Comprador':<15}{'Vendedor':<14}{'Valor':>12}  Estado")
    for r in await pool.fetch("SELECT id, comprador, vendedor, valor, estado FROM transacciones ORDER BY id"):
        print(f"{r['id']:<16}{r['comprador']:<15}{r['vendedor']:<14}{r['valor']:>12,}  {r['estado']}")
    tot = await pool.fetchrow("SELECT COUNT(*) n, COALESCE(SUM(valor),0)::BIGINT s FROM transacciones "
                              "WHERE estado='DINERO_LIBERADO'")
    print(f"\nLiberadas: {tot['n']}  Total liberado: ${tot['s']:,}")
    print("Eventos procesados (unicos):", await pool.fetchval("SELECT COUNT(*) FROM eventos_procesados"))
    print("Cambios de estado en historial:", await pool.fetchval("SELECT COUNT(*) FROM historial"))
    await pool.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--historial")
    p.add_argument("--borrar", action="store_true")
    a = p.parse_args()
    asyncio.run(main(a.historial, a.borrar))
