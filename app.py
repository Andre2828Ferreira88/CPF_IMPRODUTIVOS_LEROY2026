# app.py
import os
import uuid
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, render_template, request, send_file, flash, url_for

app = Flask(__name__)
app.secret_key = "cpf-improdutivos"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# =========================
# FUNÇÃO LEITURA ROBUSTA
# =========================
def ler_arquivo(path):
    ext = os.path.splitext(path)[1].lower()

    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)

    for sep in [";", ",", "\t"]:
        for enc in ["utf-8-sig", "UTF-16", "cp1252"]:
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc)
                if len(df.columns) > 1:
                    return df
            except:
                pass

    raise ValueError("Não foi possível ler o arquivo.")


# =========================
# PIPELINE
# =========================
def rodar_pipeline(cpf_path, mensal_path):
    # ETAPA 1
    df = ler_arquivo(cpf_path)

    df["Data do último login"] = pd.to_datetime(
        df["Data do último login"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce"
    )

    limite = datetime.now() - timedelta(days=180)
    df["Data Inferior 6 meses"] = df["Data do último login"].where(
        df["Data do último login"] < limite
    )

    cpf_part1 = os.path.join(OUTPUT_DIR, "CPF_BASE_1.csv")
    df.to_csv(cpf_part1, index=False)

    # ETAPA 2
    mensal = ler_arquivo(mensal_path)

    col_status = next(c for c in mensal.columns if "status" in c.lower())
    col_categoria = next(c for c in mensal.columns if "categoria" in c.lower())

    mensal = mensal[
        ~mensal[col_status].isin(["Cancelado", "Confirmada", "Agendada", "Distribuida"])
    ]
    mensal = mensal[
        ~mensal[col_categoria].isin(["Serviço loja", "Frete móveis planejados"])
    ]

    mensal_part1 = os.path.join(OUTPUT_DIR, "MENSAL_PART_1.csv")
    mensal.to_csv(mensal_part1, index=False)

    # ETAPA 3
    cpf = pd.read_csv(cpf_part1)
    mensal = pd.read_csv(mensal_part1)

    nomes_mensal = set(
        mensal["NOME_INSTALADOR"].dropna().astype(str).str.strip()
    )

    cpf["PRESTADOR SEM SERVIÇO"] = cpf["Nome"].apply(
        lambda x: x if str(x).strip() not in nomes_mensal else ""
    )

    cpf_part2 = os.path.join(OUTPUT_DIR, "CPF_BASE_2.csv")
    cpf.to_csv(cpf_part2, index=False)

    # ETAPA 4
    df2 = pd.read_csv(cpf_part2)

    filtro = (
        df2["Data Inferior 6 meses"].notna()
        & (df2["Data Inferior 6 meses"].astype(str).str.strip() != "")
        & df2["PRESTADOR SEM SERVIÇO"].astype(str).str.strip().ne("")
    )

    improdutivos = df2[filtro]

    final_path = os.path.join(OUTPUT_DIR, "cpf_improdutivos.csv")
    improdutivos.to_csv(final_path, index=False)

    # ETAPA 5
    final = pd.read_csv(final_path)
    final = final[~final["Recebimento de O.S"].isin(["inativo"])]
    final.to_csv(final_path, index=False)

    return final_path


# =========================
# ROTAS
# =========================
# =========================
# ROTA PRINCIPAL
# =========================
@app.route("/", methods=["GET", "POST"])
def index():
    download_url = None

    if request.method == "POST":
        cpf_file = request.files.get("cpf")
        mensal_file = request.files.get("mensal")

        if not cpf_file or not mensal_file:
            flash("Envie os dois arquivos.")
            return render_template("index.html", download_url=None)

        cpf_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{cpf_file.filename}")
        mensal_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{mensal_file.filename}")

        cpf_file.save(cpf_path)
        mensal_file.save(mensal_path)

        try:
            final_path = rodar_pipeline(cpf_path, mensal_path)
            filename = os.path.basename(final_path)
            download_url = url_for("download_file", filename=filename)
            flash("Processamento concluído com sucesso!")
        except Exception as e:
            flash(f"Erro ao processar: {e}")

    return render_template("index.html", download_url=download_url)


# =========================
# ROTA DE DOWNLOAD
# =========================
@app.route("/download/<filename>")
def download_file(filename):
    path = os.path.join(OUTPUT_DIR, filename)
    return send_file(path, as_attachment=True)


# =========================
# START
# =========================
if __name__ == "__main__":
    app.run()


