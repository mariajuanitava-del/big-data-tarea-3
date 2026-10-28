import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

carpeta = Path("/home/hadoop/resultados_sensores")
datos = []
for archivo in carpeta.glob("part-*.json"):
    for linea in archivo.read_text().splitlines():
        datos.append(json.loads(linea))
if not datos:
    raise SystemExit("No hay datos para graficar")

ultimo = max(d["window"]["start"] for d in datos)
datos = [d for d in datos if d["window"]["start"] == ultimo]
datos.sort(key=lambda d: d["sensor_id"])
sensores = [str(d["sensor_id"]) for d in datos]
temperaturas = [d["temperatura_promedio"] for d in datos]
humedades = [d["humedad_promedio"] for d in datos]
fig, ejes = plt.subplots(1, 2, figsize=(12, 5))
ejes[0].bar(sensores, temperaturas, color="coral")
ejes[0].set_title("temperatura promedio (c)")
ejes[0].set_xlabel("sensor")
ejes[1].bar(sensores, humedades, color="steelblue")
ejes[1].set_title("humedad promedio (%)")
ejes[1].set_xlabel("sensor")
fig.suptitle("inicio del minuto (UTC): " + ultimo)
plt.tight_layout()
plt.savefig("/home/hadoop/grafica_sensores.png")
plt.close()
print("grafica guardada")
