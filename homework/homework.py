"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

import zipfile
import os
import pandas as pd

def clean_campaign_data():
    import calendar

    input_path = "files/input/"
    output_path = "files/output/"

    os.makedirs(output_path, exist_ok=True)
    zip_files = [f for f in os.listdir(input_path) if f.endswith(".zip")]
    dataframes = []

    # Leer todos los CSV dentro de cada zip y concatenarlos
    for zip_filename in zip_files:
        zip_path = os.path.join(input_path, zip_filename)
        with zipfile.ZipFile(zip_path) as z:
            for file_name in z.namelist():
                if file_name.endswith(".csv"):
                    with z.open(file_name) as f:
                        df = pd.read_csv(f, sep=",")
                        dataframes.append(df)

    df_all = pd.concat(dataframes, ignore_index=True)

    # Imprime las columnas para depuración
    print("Columnas disponibles:", df_all.columns.tolist())

    # ----------------------------
    # Construir client.csv
    # ----------------------------
    client_df = df_all[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",  # Asegúrate que el nombre es correcto en los datos fuente
        ]
    ].copy()

    # Limpiar job
    client_df["job"] = client_df["job"].str.replace(".", "", regex=False)
    client_df["job"] = client_df["job"].str.replace("-", "_", regex=False)

    # Limpiar education
    client_df["education"] = client_df["education"].str.replace(".", "_", regex=False)
    client_df["education"] = client_df["education"].replace("unknown", pd.NA)

    # credit_default → 1 para "yes", 0 en cualquier otro caso
    client_df["credit_default"] = client_df["credit_default"].apply(
        lambda x: 1 if str(x).strip().lower() == "yes" else 0
    )

    # mortage → 1 para "yes", 0 en cualquier otro caso
    client_df["mortgage"] = client_df["mortgage"].apply(
        lambda x: 1 if str(x).strip().lower() == "yes" else 0
    )

    client_df.to_csv(os.path.join(output_path, "client.csv"), index=False)

    # ----------------------------
    # Construir campaign.csv
    # ----------------------------
    campaign_df = df_all[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]
    ].copy()

    # previous_outcome → 1 si "success", 0 en cualquier otro caso
    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(
        lambda x: 1 if str(x).strip().lower() == "success" else 0
    )

    # campaign_outcome → 1 si "yes", 0 en cualquier otro caso
    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(
        lambda x: 1 if str(x).strip().lower() == "yes" else 0
    )

    # Convertir mes a número (ej. jan → 01)
    month_mapping = {m.lower(): f"{i:02}" for i, m in enumerate(calendar.month_abbr) if m}
    campaign_df["month_num"] = campaign_df["month"].str.lower().map(month_mapping)

    # Crear columna last_contact_date → YYYY-MM-DD
    campaign_df["last_contact_date"] = (
        "2022-"
        + campaign_df["month_num"].astype(str)
        + "-"
        + campaign_df["day"].astype(str).str.zfill(2)
    )

    # Eliminar columnas auxiliares
    campaign_df = campaign_df.drop(columns=["day", "month", "month_num"])

    campaign_df.to_csv(os.path.join(output_path, "campaign.csv"), index=False)

    # ----------------------------
    # Construir economics.csv
    # ----------------------------
    economics_df = df_all[
        [
            "client_id",
            "cons_price_idx",
            "euribor_three_months",
        ]
    ].copy()

    economics_df.to_csv(os.path.join(output_path, "economics.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()
