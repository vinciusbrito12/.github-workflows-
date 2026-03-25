import time
import re
import json
from datetime import datetime
import os

import google.generativeai as genai
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

# =========================
# CONFIG GOOGLE SHEETS
# =========================

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

# Certifique-se de que o arquivo credenciais.json está na mesma pasta
creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais.json", scope)
client_sheets = gspread.authorize(creds)
sheet = client_sheets.open("comparador_precos").sheet1


# =========================
# CONFIG GEMINI (IA)
# =========================

# Cole aqui a sua chave gerada em: https://aistudio.google.com
GEMINI_API_KEY = "AIzaSyBk8q4rxWbzEBfMOH_XjKr0M1yTlbQ-kW4"

genai.configure(api_key=GEMINI_API_KEY)
modelo_ia = genai.GenerativeModel("gemini-1.5-flash")


# =========================
# LOG
# =========================

def salvar_log(mensagem):
    with open("log.txt", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {mensagem}\n")


# =========================
# VALIDAR PREÇO
# =========================

def preco_valido(preco):
    return preco is not None and re.search(r"\d", str(preco)) is not None


# =========================
# NORMALIZAR PREÇO
# =========================

def normalizar_preco(raw: str) -> float:
    """Converte 'R$ 1.299,90' ou '1299.90' para float."""
    clean = str(raw).replace("R$", "").strip()

    # Formato BR: 1.299,90
    if re.match(r"^\d{1,3}(\.\d{3})*,\d{2}$", clean):
        clean = clean.replace(".", "").replace(",", ".")
    else:
        clean = re.sub(r"[^\d.]", "", clean.replace(",", "."))

    try:
        return float(clean)
    except ValueError:
        return 0.0


# =========================
# EXTRAÇÃO TRADICIONAL (CASCATA)
# Alterada para priorizar a classe .sale-price do Pague Menos
# =========================

def extrair_preco_html(html: str) -> str | None:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # --- REGRA ESPECÍFICA PAGUE MENOS ---
    # Captura direto da estrutura <p class="sale-price"><strong>R$ 22,99</strong></p>
    sale_price_tag = soup.find("p", class_="sale-price")
    if sale_price_tag:
        strong_tag = sale_price_tag.find("strong")
        if strong_tag:
            return strong_tag.get_text(strip=True)
        
        # Fallback: Tenta pegar da meta tag content="22.99000" dentro do sale-price
        meta_price = sale_price_tag.find("meta", {"itemprop": "price"})
        if meta_price and meta_price.get("content"):
            valor = meta_price["content"].replace(".", ",")
            return f"R$ {valor}"

    # --- 1. JSON-LD (schema.org Product) ---
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
            if isinstance(data, list):
                data = data[0]
            if data.get("@type") == "Product":
                offers = data.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0]
                price = offers.get("price") or offers.get("lowPrice")
                if price:
                    return f"R$ {price}"
        except Exception:
            pass

    # --- 2. Meta tag Open Graph ---
    meta = soup.find("meta", {"property": "product:price:amount"})
    if meta and meta.get("content"):
        return f"R$ {meta['content']}"

    # --- 3. Atributo data-price ---
    el = soup.find(attrs={"data-price": True})
    if el:
        return f"R$ {el['data-price']}"

    # --- 4. Regex no HTML (Último recurso) ---
    match = re.search(r"R\$\s?\d{1,3}(?:\.\d{3})*(?:,\d{2})?", html)
    if match:
        return match.group()

    return None


# =========================
# EXTRAÇÃO COM GEMINI (IA)
# =========================

def extrair_preco_com_ia(html: str, nome_produto: str) -> str | None:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    
    # REMOÇÃO DE RUÍDO: Ignora botões e inputs para não confundir preço com quantidade
    for tag in soup(["script", "style", "noscript", "svg", "button", "input", "header", "footer", "nav"]):
        tag.decompose()

    texto_pagina = soup.get_text(separator="\n", strip=True)
    texto_pagina = texto_pagina[:8000] 

    prompt = (
        f"Analise o texto da página de um e-commerce para o produto: {nome_produto}\n\n"
        f"Texto extraído:\n{texto_pagina}\n\n"
        "REGRAS IMPORTANTES:\n"
        "1. Ignore números de botões de quantidade ou estoque (ex: '1', '10').\n"
        "2. Identifique o preço real de VENDA atual.\n"
        "3. Se houver preço original (DE) e promocional (POR), use o valor final (POR).\n"
        "4. Responda APENAS com o valor no formato: R$ X.XXX,XX\n"
        "5. Se não encontrar, responda: NAO_ENCONTRADO"
    )

    try:
        resposta = modelo_ia.generate_content(prompt)
        resultado = resposta.text.strip()

        if "NAO_ENCONTRADO" in resultado or not preco_valido(resultado):
            return None

        return resultado

    except Exception as e:
        salvar_log(f"{datetime.now()} - Erro na chamada Gemini: {e}")
        return None


# =========================
# SCRAPING COM PLAYWRIGHT
# =========================

def pegar_preco(url: str, nome_produto: str) -> str | None:
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="pt-BR"
            )
            page = context.new_page()

            # Bloqueia mídia para carregar mais rápido
            page.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf}", lambda route: route.abort())

            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            # Aguarda um pouco para o JS renderizar o preço
            time.sleep(3)
            
            # Scroll leve para garantir que o conteúdo carregue
            page.evaluate("window.scrollTo(0, 400)")

            html = page.content()
            browser.close()

        # --- Tentativa 1: Extração tradicional com regra específica ---
        preco = extrair_preco_html(html)
        if preco_valido(preco):
            print(f"    [sucesso] encontrou → {preco}")
            return preco

        # --- Tentativa 2: Gemini como Fallback ---
        print(f"    [tradicional falhou] → acionando Gemini...")
        preco_ia = extrair_preco_com_ia(html, nome_produto)
        if preco_valido(preco_ia):
            print(f"    [Gemini] encontrou → {preco_ia}")
            return preco_ia

        return None

    except Exception as e:
        salvar_log(f"Erro em {url}: {e}")
        return None


# =========================
# RETRY AUTOMÁTICO
# =========================

def tentar_3x(func, *args):
    for tentativa in range(1, 4):
        resultado = func(*args)
        if resultado:
            return resultado
        salvar_log(f"Tentativa {tentativa}/3 sem resultado para: {args[0]}")
        time.sleep(3)
    return None


# =========================
# LISTA DE PRODUTOS
# =========================

produtos = [
    {
        "nome": "Arroz Branco Prato Fino 5kg",
        "mercado": "Pague Menos",
        "url": "https://www.superpaguemenos.com.br/arroz-prato-fino-tipo-1-5kg/p"
    },
    {
        "nome": "Arroz Branco Prato Fino 5kg",
        "mercado": "Covabra",
        "url": "https://www.covabra.com.br/arroz-prato-fino-tipo-i-5kg/p"
    }
]


# =========================
# COLETA E COMPARAÇÃO
# =========================

dados_coletados = []

for produto in produtos:
    print(f"\n🔍 Buscando: {produto['nome']} em {produto['mercado']}...")
    preco_raw = tentar_3x(pegar_preco, produto["url"], produto["nome"])

    if preco_valido(preco_raw):
        linha = [
            produto["nome"],
            produto["mercado"],
            preco_raw,
            datetime.now().strftime("%Y-%m-%d %H:%M")
        ]
        sheet.append_row(linha)
        dados_coletados.append(linha)
        print(f"  ✅ {produto['mercado']}: {preco_raw}")
    else:
        print(f"  ❌ Não encontrado em {produto['mercado']}")
    
    time.sleep(2)

if dados_coletados:
    df = pd.DataFrame(dados_coletados, columns=["Produto", "Mercado", "Preço", "Data"])
    df["Preço_num"] = df["Preço"].apply(normalizar_preco)
    
    print("\n📊 TABELA COMPARATIVA:")
    print(df[["Produto", "Mercado", "Preço"]].to_string(index=False))

    menores = df.loc[df.groupby("Produto")["Preço_num"].idxmin()]
    print("\n🏆 MELHOR PREÇO ENCONTRADO:")
    for _, row in menores.iterrows():
        print(f"  {row['Produto']} → {row['Mercado']}: {row['Preço']}")
else:
    print("\n⚠️ Nenhum preço coletado. Verifique o arquivo log.txt.")

    # =========================
# GERAR ABA DE COMPARAÇÃO NO SHEETS
# =========================
try:
    # Tenta abrir ou criar a aba 'Comparativo'
    try:
        sheet_comp = client_sheets.open("comparador_precos").worksheet("Comparativo")
        sheet_comp.clear() # Limpa para atualizar
    except:
        sheet_comp = client_sheets.open("comparador_precos").add_worksheet(title="Comparativo", rows="100", cols="20")

    # Transforma os dados coletados em formato horizontal (Pivot)
    df_pivot = df.pivot_table(index='Produto', columns='Mercado', values='Preço', aggfunc='first').reset_index()
    
    # Preenche valores vazios e envia para o Sheets
    df_pivot = df_pivot.fillna("N/A")
    sheet_comp.update([df_pivot.columns.values.tolist()] + df_pivot.values.tolist())
    print("\n✅ Aba 'Comparativo' atualizada com sucesso!")
except Exception as e:
    print(f"\n⚠️ Não foi possível atualizar a aba horizontal: {e}")