import asyncio
import os  
import pandas as pd
from datetime import timedelta, datetime
from crawlee.crawlers import PlaywrightCrawler
from src.routes import handler_google_maps

async def main():
    crawler = PlaywrightCrawler(
        headless=False,
        max_requests_per_crawl=10, 
        request_handler_timeout=timedelta(minutes=10), 
    )

    crawler.router.default_handler(handler_google_maps)

    await crawler.run(["https://www.google.com/maps?hl=es"])

    print("📦 Procesando datos...")  
    dataset = await crawler.get_data()
    
    if dataset.items:
        df = pd.DataFrame(dataset.items)
        
        total_antes = len(df)
        df = df.drop_duplicates(subset=['Nombre', 'Direccion'], keep='first')
        total_despues = len(df)
        print(f"✨ Se eliminaron {total_antes - total_despues} duplicados.")
        carpeta_salida = "Resultados"
        
        if not os.path.exists(carpeta_salida):
            os.makedirs(carpeta_salida)
            print(f"📁 Carpeta '{carpeta_salida}' creada.")

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        nombre_archivo = f"Reporte_Pizzerias_{timestamp}.xlsx"
        ruta_completa = os.path.join(carpeta_salida, nombre_archivo)
        try:
            df.to_excel(ruta_completa, index=False)
            ruta_absoluta = os.path.abspath(ruta_completa)
            print(f"✅ ¡ÉXITO! Archivo guardado/actualizado en:\n   👉 {ruta_absoluta}")
            print(f"📊 Total Leads Únicos: {total_despues}")

        except PermissionError:
            print("\n❌ ERROR CRÍTICO: No se pudo guardar el archivo.")
            print(f"⚠️  MOTIVO: Tienes abierto el archivo '{nombre_archivo}'.")
            print("💡 SOLUCIÓN: Cierra el Excel y vuelve a ejecutar el programa.")
            
    else:
        print("⚠️ No hay datos.")

if __name__ == '__main__':
    asyncio.run(main())