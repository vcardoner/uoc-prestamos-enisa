from utils import enisa_scraper, twitter_scraper, input_linkedin, linkedin_scraper, input_chromedriver, \
    infocif_scraper, axesor_scraper, genera_csv


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # En primer recuperamos inputs necesarios:
    # 1. Credenciales de Linkedin
    # 2. Ubicación chromedriver para Selenium
    user, pwd = input_linkedin()
    path = input_chromedriver()

    # Hacemos scraping de los datos centrales del proceso, de ENISA
    df_enisa = enisa_scraper()

    # Recuperamos el CIF de las empresas vía Infocif.es
    df_infocif = infocif_scraper(df_enisa)

    # Recuperamos los campos de CNAE, dirección postal y SIC de Axesor a partir de CIF
    df_axesor = axesor_scraper(df_infocif, path)

    # Extraemos indicadores de los datos de Twitter
    df_twitter, df_twitter_enisa = twitter_scraper(df_enisa)

    # Recuperar datos de la empresa en Linkedin
    df_linkedin = linkedin_scraper(user, pwd, df_enisa, path)

    # Unimos la información y guardamos el dataframe completo en un csv
    genera_csv(df_enisa, df_infocif, df_axesor, df_twitter, df_twitter_enisa, df_linkedin)
