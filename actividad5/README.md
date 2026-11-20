# Actividad 5 – ESCROW con RabbitMQ y PostgreSQL (Neon)

Cada participante es un **proceso independiente** que se comunica solo por RabbitMQ.
El estado de las transacciones se guarda en **PostgreSQL**: en Neon o en un contenedor local.

## 1. Base de datos

**Opción A – Neon:**
1. Crear un proyecto en https://neon.tech y copiar la *connection string* (Dashboard → Connect).
2. Definir la variable en **cada terminal** donde corran el ESCROW:
   ```powershell
   # Windows PowerShell
   $env:DATABASE_URL="postgresql://usuario:clave@ep-xxxx.neon.tech/neondb?sslmode=require"
   ```
   ```bash
   # Linux / Mac
   export DATABASE_URL="postgresql://usuario:clave@ep-xxxx.neon.tech/neondb?sslmode=require"
   ```
   Las tablas se crean solas la primera vez (`bd.py`). 

**Opción B – local:** `docker compose up -d` ya incluye PostgreSQL; si no definen `DATABASE_URL`, se usa ese.

## 2. Ejecutar

```bash
pip install -r requirements.txt
docker compose up -d rabbitmq        # (o "docker compose up -d" para RabbitMQ + PostgreSQL local)

# una terminal por participante
python servicio_escrow.py
python pasarela.py
python vendedor.py
python comprador.py --n 10           # 10 compradores, 10 transacciones con valores distintos

python ver_estado.py                         # estado guardado en PostgreSQL
python ver_estado.py --historial TX-XXXX-001 # cambios de estado de una transacción
python ver_estado.py --borrar                # vaciar las tablas antes de una prueba
```

En Neon también pueden ver los datos en el **SQL Editor**: `SELECT * FROM transacciones;`

