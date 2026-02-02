from crawlee.crawlers import PlaywrightCrawlingContext
from urllib.parse import unquote

async def handler_google_maps(context: PlaywrightCrawlingContext):
    page = context.page
    print(f"📍 Abriendo mapa...")

    await page.locator('#UGojuc').wait_for()
    busqueda = context.request.user_data.get("busqueda", "Pizzerias en Lima")
    print(f"🔎 Buscando: '{busqueda}'...")
    
    await page.locator('#UGojuc').fill(busqueda)
    await page.keyboard.press("Enter")

    print("⏳ Cargando lista...")
    await page.locator('div[role="feed"]').wait_for(timeout=15000)
    sidebar = page.locator('div[role="feed"]')
    await sidebar.hover()
    
    for i in range(5):
        print(f"   ⬇️ Scroll {i+1}/5...")
        await page.mouse.wheel(0, 5000)
        await page.wait_for_timeout(2000)

    print("✅ Iniciando extracción BLINDADA...")
    cards = await page.locator('div[role="article"]').all()
    print(f"📊 Total encontrados: {len(cards)}")

    count = 0
    
    ultimo_telefono_real = ""
    ultima_direccion_real = ""
    ultima_web_real = ""
    nombre_anterior = ""

    for i, card in enumerate(cards):
        try:
            await card.scroll_into_view_if_needed()
            
            titulo_el = card.locator('.fontHeadlineSmall').first
            nombre_lista = await titulo_el.inner_text()
            if nombre_lista == nombre_anterior:
                print(f"      ⚠️ Saltando duplicado visual: {nombre_lista}")
                continue

            url_antes_click = page.url

            try:
                await titulo_el.click(force=True)
            except:
                await card.click(force=True)

            waited = 0
            nombre_final = nombre_lista
            while waited < 30: 
                h1_panel = page.locator('h1.DUwDvf')
                if await h1_panel.count() > 0:
                    texto_panel = await h1_panel.first.inner_text()
                    if texto_panel == nombre_lista:
                        nombre_final = texto_panel
                        break
                await page.wait_for_timeout(100)
                waited += 1

            if nombre_final == nombre_anterior:
                continue

            telefono = "No tiene"
            intentos = 0
            while intentos < 15: 
                phone_loc = page.locator('button[data-item-id^="phone"]')
                temp = "No tiene"
                if await phone_loc.count() > 0:
                    raw = await phone_loc.get_attribute("aria-label")
                    temp = raw.replace("Teléfono: ", "").strip()
                
                if temp != "No tiene" and temp == ultimo_telefono_real:
                    await page.wait_for_timeout(100)
                    intentos += 1
                else:
                    telefono = temp
                    break
            
            if telefono == ultimo_telefono_real and telefono != "No tiene": telefono = "No tiene"

            direccion = "No detectada"
            intentos = 0
            while intentos < 15:
                addr_loc = page.locator('button[data-item-id="address"]')
                temp = "No detectada"
                if await addr_loc.count() > 0:
                    raw = await addr_loc.get_attribute("aria-label") or ""
                    temp = raw.replace("Dirección: ", "").strip()
                
                if temp != "No detectada" and temp == ultima_direccion_real:
                    await page.wait_for_timeout(100)
                    intentos += 1
                else:
                    direccion = temp
                    break
            
            if direccion == ultima_direccion_real and direccion != "No detectada": direccion = "No detectada"

            web = "No tiene"
            intentos = 0
            while intentos < 15:
                web_loc = page.locator('a[data-item-id="authority"]')
                temp = "No tiene"
                if await web_loc.count() > 0:
                    raw_url = await web_loc.get_attribute("href") or "No tiene"
                    if "google.com/url" in raw_url:
                        try:
                            temp_clean = raw_url.split("q=")[1].split("&")[0]
                            temp = unquote(temp_clean)
                        except:
                            temp = raw_url
                    else:
                        temp = raw_url
                
                if temp != "No tiene" and temp == ultima_web_real:
                    await page.wait_for_timeout(100)
                    intentos += 1
                else:
                    web = temp
                    break
            
            if web == ultima_web_real and web != "No tiene": web = "No tiene"

            url_final = page.url
            intentos = 0
            while intentos < 20: 
                if url_final == url_antes_click:
                    await page.wait_for_timeout(100)
                    url_final = page.url
                    intentos += 1
                else:
                    break
            
            if url_final == url_antes_click:
                url_final = "URL no actualizada (Posible error de carga)"
            

            print(f"   👉 {i+1}/{len(cards)}: {nombre_final} | 📞 {telefono}")

            nombre_anterior = nombre_final
            
            if telefono != "No tiene": ultimo_telefono_real = telefono
            if direccion != "No detectada": ultima_direccion_real = direccion
            if web != "No tiene": ultima_web_real = web

            await context.push_data({
                "Busqueda": busqueda,
                "Nombre": nombre_final,
                "Telefono": telefono,
                "Direccion": direccion,
                "Web": web,
                "Url_Mapa": url_final
            })
            count += 1

        except Exception as e:
            pass

    print(f"🏁 ¡FIN! DATOS VALIDADOS: {count}")