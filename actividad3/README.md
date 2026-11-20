# Actividad 3 – Múltiples transacciones simultáneas

| Archivo | Qué es |
|---|---|
| `escrow_multi.py` | Código: 10 compradores, 10 vendedores, 10 transacciones ESCROW |
| `graficas.py` | Genera las 3 figuras a partir de los CSV de `evidencias/` |
| `evidencias/10tx_*` | Corrida principal (10 transacciones): bitácora de eventos, resumen por transacción y salida de consola |
| `evidencias/carga_100tx_*` y `evidencias/carga_1000tx_0ms_*` | Pruebas de carga sin costo por evento y con 100 transacciones a 5 ms |
| `evidencias/carga_1000tx_*consumidores_*` | Pruebas de carga con 1.000 transacciones, 5 ms por evento y 1, 4 y 16 consumidores ESCROW |
| `evidencias/fig*.png` | Gráficas que van en el informe |

```bash
python escrow_multi.py                       # 10 transacciones -> evidencias/10tx_*
python escrow_multi.py --n 1000 --costo-ms 5 --workers 4 --quiet --out evidencias/carga_1000tx_4consumidores
python graficas.py
```
