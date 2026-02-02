import asyncio
import pandas as pd
import re
import os
# Importamos lo necesario de Crawlee
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request

# Diccionario global para ir guardando los emails que encontremos
# Clave: Índice del Excel, Valor: Email encontrado
emails_encontrados = {}

async def handler_email_hunter(context: PlaywrightCrawlingContext):
    """
    Este robot entra a una web, busca arrobas (@) y se va.
    """
    page = context.page
    # Recuperamos el índice de la fila para saber a qué pizzería pertenece
    indice = context.request.user_data.get("indice")
    url_actual = context.request.url

    print(f"🌍 Escaneando: {url_actual}")

    try:
        # 1. Carga Rápida (Solo esperamos que aparezca el texto, no las imágenes)
        # Timeout de 10 segundos para no quedarnos pegados
        await page.wait_for_load_state("domcontentloaded", timeout=10000)

        # 2. Extraer todo el contenido (HTML + Texto visible)
        content_html = await page.content()
        content_text = await page.inner_text("body")

        # 3. La Regex Maestra (Busca patrones de email)
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        # Buscamos en ambos lugares
        found_in_html = set(re.findall(email_pattern, content_html))
        found_in_text = set(re.findall(email_pattern, content_text))
        
        # Unimos resultados
        todos_emails = found_in_html.union(found_in_text)
        
        # 4. Limpieza (Quitamos basura como 'imagen@2x.png')
        lista_limpia = []
        ext_basura = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.js', '.css']
        
        for email in todos_emails:
            email = email.lower()
            # Si el "email" termina en una extensión de imagen, es falso.
            if not any(email.endswith(ext) for ext in ext_basura):
                lista_limpia.append(email)

        # 5. Guardar si encontramos algo
        if lista_limpia:
            resultado = ", ".join(lista_limpia)
            print(f"   🎉 ¡BINGO! -> {resultado}")
            emails_encontrados[indice] = resultado
        else:
            print("   💨 Nada por aquí.")

    except Exception as e:
        # Si la web falla, imprimimos error pequeño y seguimos. NO ROMPEMOS EL PROGRAMA.
        print(f"   ⚠️ Web fallida ({url_actual}): {e}")

async def main():
    # --- 1. CARGAR EL EXCEL ---
    carpeta = "Resultados"
    archivo_nombre = "resultados_pizzerias.xlsx"
    ruta_excel = os.path.join(carpeta, archivo_nombre)

    if not os.path.exists(ruta_excel):
        print(f"❌ ERROR: No encuentro el archivo '{ruta_excel}'.")
        print("   Primero ejecuta la opción [1] para buscar negocios.")
        return

    print(f"📂 Leyendo: {ruta_excel}...")
    try:
        df = pd.read_excel(ruta_excel)
    except Exception as e:
        print(f"❌ No pude leer el Excel. ¿Quizás está corrupto? Error: {e}")
        return

    if "Web" not in df.columns:
        print("❌ El Excel no tiene la columna 'Web'.")
        return

    # --- 2. PREPARAR OBJETIVOS ---
    lista_requests = []
    
    print("⚙️ Filtrando webs válidas...")
    
    for i, fila in df.iterrows():
        web = str(fila["Web"]).strip()
        
        # Filtramos los "No tiene", "nan", vacíos...
        valores_invalidos = ["no tiene", "nan", "none", "no detectada", ""]
        
        if web.lower() not in valores_invalidos:
            # Asegurar protocolo http
            if not web.startswith("http"):
                web = "http://" + web
            
            # Crear la misión para el robot
            req = Request.from_url(web, user_data={"indice": i})
            lista_requests.append(req)

    total_webs = len(lista_requests)
    print(f"🎯 Se encontraron {total_webs} webs para escanear.")
    print("-------------------------------------------------------")

    if total_webs == 0:
        print("⚠️ Ningún negocio tiene página web. Terminando.")
        return

    # --- 3. LANZAR ROBOT ---
    crawler = PlaywrightCrawler(
        headless=True,
        max_requests_per_crawl=100,
        request_handler_timeout=15000, # 15 segs máx por web
    )

    crawler.router.default_handler(handler_email_hunter)
    
    # ¡A CAZAR!
    await crawler.run(lista_requests)

    # --- 4. GUARDAR RESULTADOS ---
    print("\n💾 Guardando hallazgos en el Excel...")

    # Crear columna si no existe
    if "Email_Hunter" not in df.columns:
        df["Email_Hunter"] = "No encontrado"

    # Rellenar datos
    encontrados_count = 0
    for idx, emails in emails_encontrados.items():
        df.at[idx, "Email_Hunter"] = emails
        encontrados_count += 1

    # Guardado seguro (Anti-Bloqueo de Windows)
    try:
        df.to_excel(ruta_excel, index=False)
        print(f"✅ ¡ÉXITO! Se encontraron emails para {encontrados_count} negocios.")
        print(f"📄 Revisa el archivo: {ruta_excel}")
    except PermissionError:
        print("\n❌ ERROR CRÍTICO DE GUARDADO")
        print("⚠️  El archivo Excel está ABIERTO.")
        print("👉 Ciérralo y vuelve a ejecutar solo esta opción [2] para guardar.")

if __name__ == '__main__':
    asyncio.run(main())