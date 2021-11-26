import configparser

class MetodosDic():
    """
            Clase que interactúa con los diccionarios, ya sea internamente en un codigo, o a través de un archivo de
            configuración (como config.ini)
    """

    def anyadir_dic(self, nombreDiccionario, diccionario, lista_dics, lista_nom_dics):
        """
                Permite añadir diccionarios tanto a la dista de diccionarios como a su nombre. Se debe introducir
                obligatoriamente el nombre del diccionario, el diccionario, la lista de diccionarios donde se insertan
                los diccionarios desde el diccionario a seleccionar, y, finalmente, la lista de los nombres de los
                diccionarios para así poder registrar sus nombres.
        """
        lista_nom_dics.append(nombreDiccionario)
        lista_dics.append(diccionario)

    def exportar_multiples_dics(self, lista_dics, lista_nom_dics):
        """
                Permite exportar los diccionarios desde la lista de diccionarios y la lista de nombre de los
                diccionarios con el fin de irlos escribiendo en el archivo de configuración correspondiente.
        """
        contador = 0
        for lista_n in range(len(lista_dics)):
            nombre = str(lista_nom_dics[contador]).replace("['", "").replace("']", "")
            self.exportar_dicts_a_config_ini((lista_dics[contador]), nombre)
            contador += 1

    def exportar_dicts_a_config_ini(self, diccionario, nombreDiccionario):
        """
                Permite escribir de manera individual los diccionarios en el archivo de configuración a través de los
                métodos de configParser.
        """

        contador = 0
        config = configparser.ConfigParser()
        config.read('config.ini')
        config.add_section(nombreDiccionario)
        for lista_01 in diccionario:
            acumulador = ""
            verificador = str(type(diccionario[lista_01]))
            for lista_01_a in (list(diccionario.values())[contador]):
                #Si, uno de los diccionarios, contiene un string en lugar de una lista, este directamente lo verifica
                #para así poder juntar los caracteres del string.
                if acumulador == "":
                    acumulador = lista_01_a
                else:
                    if verificador == '''<class 'str'>''':
                        acumulador += str(lista_01_a)
                    else:
                        acumulador += str(", " + lista_01_a)
                config.set(str(nombreDiccionario), str(lista_01), str(acumulador))
            contador += 1
        with open('config.ini', 'w') as configfile:
            config.write(configfile)

    def importar_dicts_de_config_ini (self, config_file):
        """
                Permite importar los diccionarios a través de un archivo de configuracción seleccionado con
                anterioridad.
        """

        pru = configparser.ConfigParser()
        pru.read(config_file)
        d = pru.__dict__['_sections'].copy()
        return d
