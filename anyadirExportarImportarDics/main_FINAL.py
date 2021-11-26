import os
import configparser
from JFRP.metodos_dic import *

if __name__ == '__main__':


    #
    # Primero se declaran los diccionarios a exportar y las listas de diccionarios y de sus nombres para así poder
    # trabajar con ellos.
    #

    lista_ddic = [
        {'diccionarioJFRP': {'HOLA': 'testa', 'QUE_TAL': 'muy bien', 'ESTUPENDO': ['ME_ALEGRO', 'MUCHO', 'JEJE']}}, {
            "estructuraCarpetas": {
                'SALL': ['CCLOAN', 'GLOBALCAJA', 'IBERIA CARDS', 'LISBOA', 'OKUANT', 'SAF', 'IBERIA CARDS SERIE B',
                         'IBERIA PLEITOS MASA', 'SAF1', 'WENANCE', 'UNICAJA', 'MONEYMAS', 'GLOBALCAJA2'],
                'GPA': ['CCLOAN', 'GALIA', 'HILTI', 'IBERIA CARDS', 'LISBOA', 'OKUANT'],
                'BUH': 'testa'}}]

    lista_dics = []
    lista_nom_dics = []

    #
    # Esto es una prueba para saber la localización del directorio donde se encuentra acutalmente el archivo raíz. Esto
    # es OPCIONAL y, por ende, puede borrarse sin problemas.
    #

    print(os.getcwd())

    if not (os.path.isdir('JFRP')):
        os.mkdir('JFRP')
    os.chdir('JFRP')

    #
    # Se resetea el archivo config.ini por si tenía datos ya existentes
    #

    archivo_custom = open('config.ini', 'w')
    archivo_custom.close()
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Se muestran por acá las funciones de añadir, exportar e importar diccionarios.

    for contador in range(len(lista_ddic)):
        anyadir_dict = MetodosDic()
        anyadir_dict.anyadir_dic(
            str(list(lista_ddic[contador].keys())).replace("['", "").replace("']", "").replace("(", "").replace(")",
                                                                                                                ""),
            list(lista_ddic[contador].values())[0], lista_dics, lista_nom_dics)

    exportar = MetodosDic()
    exportar.exportar_multiples_dics(list(lista_dics), list(lista_nom_dics))

    importar = MetodosDic()
    dic_importado = importar.importar_dicts_de_config_ini('config.ini')

    # Se imprime el diccionario importado del arcihvo de configuracción para comprobar que todo funciona a la perfección

    print(dic_importado)

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
