import requests
import elementpath
import xml.etree.ElementTree as ET
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe


chile = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CHL.xml"
irlanda = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_IRL.xml"
costa_rica = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CRI.xml"
argentina = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_ARG.xml"
lituania = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_LTU.xml"
dinamarca = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_DNK.xml"
paises = [chile, irlanda, costa_rica, argentina, lituania, dinamarca]
df_cols = ["COUNTRY", "YEAR", "GHO", "SEX", "Numeric", "Display", "AGEGROUP", "GHECAUSES", "Low", "High"]
rows = []
utiles = ["Number of deaths", "Number of infant deaths", "Number of under-five deaths",
          "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
          "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
          "Estimates of number of homicides", "Crude suicide rates (per 100 000 population)",
          "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
          "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
          "Estimated road traffic death rate (per 100 000 population)", "Estimated number of road traffic deaths",
          "Mean BMI (crude estimate)", "Mean BMI (age-standardized estimate)",
          "Prevalence of obesity among adults, BMI > 30 (age-standardized estimate) (%)",
          "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
          "Prevalence of overweight among adults, BMI > 25 (age-standardized estimate) (%)",
          "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
          "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",
          "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",
          "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
          "Estimate of daily cigarette smoking prevalence (%)", "Estimate of daily tobacco smoking prevalence (%)",
          "Estimate of current cigarette smoking prevalence (%)", "Estimate of current tobacco smoking prevalence (%)",
          "Mean systolic blood pressure (crude estimate)", "Mean fasting blood glucose (mmol/l) (crude estimate)",
          "Mean Total Cholesterol (crude estimate)", "Mean BMI (kg/m&#xb2;) (crude estimate)",
          "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
          "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (crude estimate) (%)",
          "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)"]


for pais in paises:
    print("Cargando datos...")
    response = requests.get(pais)
    root = ET.fromstring(response.content)
    for child in root:
        incluir = False
        try:
            s_gho = child.find("GHO").text
        except AttributeError:
            s_gho = "None"
        else:
            if s_gho in utiles:
                incluir = True

        try:
            s_country = child.find("COUNTRY").text
        except AttributeError:
            s_country = "None"

        try:
            s_year = child.find("YEAR").text
        except AttributeError:
            s_year = "None"

        try:
            s_ghecauses = child.find("GHECAUSES").text
        except AttributeError:
            s_ghecauses = "None"

        try:
            s_agegroup = child.find("AGEGROUP").text
        except AttributeError:
            s_agegroup = "None"

        try:
            s_sex = child.find("SEX").text
        except AttributeError:
            s_sex = "None"

        try:
            s_numeric = child.find("Numeric").text
        except AttributeError:
            s_numeric= "None"

        try:
            s_display = child.find("Display").text
        except AttributeError:
            s_display = "None"

        try:
            s_low = child.find("Low").text
        except AttributeError:
            s_low = "None"

        try:
            s_high = child.find("High").text
        except AttributeError:
            s_high = "None"


        if incluir:
            rows.append({"COUNTRY": s_country, "YEAR": s_year, "GHO": s_gho, "SEX": s_sex, "Numeric": s_numeric,
                     "Display": s_display, "AGEGROUP": s_agegroup, "GHECAUSES": s_ghecauses, "Low": s_low,
                     "High": s_high})


out_df = pd.DataFrame(rows, columns=df_cols)

# ACCES GOOGLE SHEET
gc = gspread.service_account(filename="taller-tarea-4-316418-7cac57470c56.json")
sh = gc.open_by_key("18EO2-ZVkEN7PNyMaClr7p1OUuQ57xedpKZO-xh7tKQ0")
worksheet = sh.get_worksheet(0)

# APPEND DATA TO SHEET
worksheet.clear()
set_with_dataframe(worksheet, out_df)
