# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# UNIVERSIDADE FEDERAL DE CAMPINA GRANDE
# PRÓ-REITORIA DE PÓS-GRADUAÇÃO
# COORDENAÇÃO GERAL DE PESQUISA
# PYLATTES - Pacote para leitura de currículos Lattes em lote, no formato XML.
# Fornece ferramenta para ler e parse as informações em dataframe
# permite realização de sumários e gráficos, com possibilidade de exportação
# Atribui nota para os currículos com base em padrão da UFCG.
# ---------------------------------------------------------------------------
# Autor: Luciano Barosi
# Criado: 14.04.2020
# v1.0
#----------------------------------------------------------------------------
# Última Alteração
# BAROSI: 22.06.2020
# BAROSI: 23.07.2020
# BAROSI: 24.07.2020
#----TODO
#----3. Verificar função dfTidy, algumas colunas parecem com problemas, provavelmente é a colskeep com problema.
#----6.  Função gera pontuação
#----7.  funções para exportar as produções da UFCG
#----10. CLI e argparse
#----------------------------------------------------------------------------
# INICIALIZAÇÃO
#----------------------------------------------------------------------------
# Manipulação de Arquivos
import os
import sys
import traceback
from sys import exit
from glob import glob
# Utilidades
from itertools import compress
from itertools import repeat
from datetime import datetime as dt
from os.path import join
from os import access, R_OK
from os.path import isfile
from collections import OrderedDict
# Pacotes básicos
import numpy as np
import pandas as pd
# Parsing de XML em ElementTree
# lxml bem mais rapido que ElementTree
from lxml import etree
from lxml.etree import ParseError
from lxml.etree import ParserError
# Validar ISSN e ISBN
from stdnum import isbn
from stdnum import issn
# NOrmalizar strings para comparação sem acentos
from unidecode import unidecode
from crossref.restful import Works
from crossref.restful import CrossrefAPIError
#----------------------------------------------------------------------------
#-----------------------Funções de Módulo------------------------------------
def pathHandler(PATH=""):
    """Função global do módulo para construir caminhos relativos ao caminho do módulo

    Args:
        PATH (type): Caminho relativo `PATH`. Defaults to "".

    Returns:
        type: Caminho absoluto

    """
    parentDir = os.path.dirname(__file__)
    newPath = os.path.join(parentDir, os.path.abspath(PATH))
    return newPath
#----------------------------------------------------------------------------
# Preparando logger no Módulo
#----------------------------------------------------------------------------
# import logging
# from logging.handlers import TimedRotatingFileHandler
# def get_file_handler():
#     LOG_FILE = pathHandler("../../reports/Logs/pylattes.log")
#     formato = logging.Formatter("%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
#     file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
#     file_handler.setFormatter(formato)
#     return file_handler
# def get_logger(logger_name):
#     logger = logging.getLogger(logger_name)
#     logger.setLevel(logging.DEBUG)
#     logger.addHandler(get_file_handler())
#     logger.propagate = False
#     return logger
# Inicia Log
#LOG = get_logger(__name__)
#----------------------------------------------------------------------------

#----------------------------------------------------------------------------
def readFolder(PATH="../../data/raw/CVs"):
    """Le todos os arquivos XML que estiverem no caminho indicado e em subpastas.

    Args:
        PATH (type): Caminho para arquivos XML de Curriculos Lattes `PATH`. Defaults to "../../data/raw/CVs".

    Returns:
        type: Lista com o nome dos arquivos em path absoluto.

    """
    path = pathHandler(PATH)
    maskCV = str(path) + "/*.xml"
    try:
        files = glob(maskCV)
    except OsError as error:
        #LOG.error("readFolder: %s", error)
        sys.exit(error)
    n = len(files)
    #LOG.info("readFolder: %s Arquivos XML lidos", n)
    return files

def makeDBnomes(PATH, save_to_disk = False):
    """Short summary.

    Args:
        PATH (type): Description of parameter `PATH`.
        save_to_disk (type): Description of parameter `save_to_disk`. Defaults to False.

    Returns:
        type: Description of returned object.

    """
    #----Le todas as pastas e subpastas indicadas para busca de XML
    files = readFolder(PATH)
    nomes = []
    problemas = []

    for file in files:
        NOME = []
        CPF = []
        ID = []
        try:
            #----XML funciona?
            tree = etree.parse(file)
        except etree.XMLSyntaxError:
            #LOG.error("XML inválido: %s", file)
            problemas.append([file, ID, CPF, NOME])
        except (ParserError, ParseError) as error:
            #LOG.error("XML inválido: %s", file)
            problemas.append([file, ID, CPF, NOME])
        else:
            root = tree.getroot()
            try:
                CPF = root.find('DADOS-GERAIS').attrib['CPF']
            except (KeyError, AttributeError) as error:
                CPF = np.nan
            try:
                NOME = root.find('DADOS-GERAIS').attrib['NOME-COMPLETO']
            except (KeyError, AttributeError) as error:
                NOME = np.nan
            try:
                ID = root.attrib['NUMERO-IDENTIFICADOR']
            except (KeyError, AttributeError) as error:
                #LOG.error("XML inválido: %s", file)
                problemas.append([file, ID, CPF, NOME])
                ID = np.nan
            nomes.append([file, ID, CPF, NOME])

    if len(nomes) != 0:
        df = pd.DataFrame(nomes)
        df.columns = ['FILE','ID', 'CPF','NOME']
        if len(problemas) !=0:
            dfProblema = pd.DataFrame(problemas)
            dfProblema.columns = ['FILE','ID', 'CPF','NOME']
        else:
            df = pd.DataFrame()
        #----Apenas nome do arquivo dentro do pacote
        df['FILE']= df['FILE'].apply( lambda val: '../../' + '/'.join(val.split('/')[-5:]))
        #----Nome completo, tudo em maíusculas e sem acentos, normalizado para comparações.
        dfRuim = df[ ( (df['NOME'].isna()) | (df['CPF'].isna()) | (df['ID'].isna()) )]
        dfRuim = pd.concat([dfRuim, dfProblema], axis = 0)
        df = df.dropna()
        df = df.groupby(['NOME']).first().reset_index()
        df['NOME']=df['NOME'].apply(lambda val: unidecode(val.upper()))
        #----Salva arquivo XLSX
        if save_to_disk:
            PATH = pathHandler('../../data/external/DBnomes.xlsx')
            PATHRuim = pathHandler('../../data/external/XMLruim.xlsx')
            df.to_excel(PATH, index=False)
            dfRuim.to_excel(PATHRuim, index=False)

    return df, dfRuim

#----------------------------------------------------------------------------
#------------------------- CLASSE cvPesquisador------------------------------
#----------------------------------------------------------------------------
class Pesquisador:
    """Classe Pesquisador, representa todas as informações que podemos obter sobre um pesquisador com os bancos de dados disponívels: Lattes, UFCG, SAAP. As propriedades file, nome, periodo são ajustadas com método de getters e setters.

    Args:
        file (type): Caminho do arquivo do XML do Lattes `file`. Defaults to None.
        nome (type): Caminho do nome do pesquisador. Apenas `nome` ou `file` deve ser utilizado. `nome`. Defaults to None.
        periodo (type): Período para avaliação dos currículos `periodo`. Defaults to None.
        **kwargs (type): Caminhos de arquivos adicionais, informações sobre o número de colunas adicionais, conforme documentação mais detalhada. `**kwargs`.

    Attributes:
        __file (type): é definido chamando a função setter apropriada `__file`.
        __nome (type): é definido chamando a função setter apropriada `__nome`.
        __periodo (type): é definido chamando a função setter apropriada `__periodo`.
        __UFCG (type): Flag booleano controla se serão utilizados os dados da UFCG sobre lotação e SIAPE `__UFCG`. É definido se for fornecido o caminho para o arquivo.
        __SAAP (type): Flag booleano controla se serão utilizados os dados do SAAP É definido se for fornecido o caminho para o arquivo. `__SAAP`.
        __PONTUA (type): Flag booleano controla se serão pontuados os currículos. É definido se for fornecido o caminho para o arquivo. `__PONTUA`.
        kwargs (type): Description of parameter `kwargs`.

    """
    #----Métodos-ATributos
    def __init__(self,file=None, nome=None, periodo=None, **kwargs):

        self.__file = file
        self.__nome = nome
        self.__periodo = periodo
        self.__UFCG = True
        self.__SAAP = True
        self.__PONTUA = True
        self.kwargs = kwargs
        pass

    #----Setters e Getters via @property
    @property
    def nome(self):
        return self.__nome

    @nome.setter
    def nome(self, value):
        self.__nome = value

    @property
    def file(self):
        return self.__file

    @file.setter
    def file(self, value):
        self.__file = value

    @property
    def periodo(self):
        return self.__periodo

    @periodo.setter
    def periodo(self, value):
        self.__periodo = value
    #--------------------------------------------------
    #----Validações
    @staticmethod
    def defaultValues():
        """ método estático: Define os valores padrões para todos os caminhos de arquivos necessários.

        Returns:
            type: Retorna um dicionário com os caminhos.

        """
        kwDefault = {'pathCAPES' : '../data/TABELA_EQUIVALENCIA_CNPQ_CAPES.csv',
                    'pathQualis' : '../data/Qualis_2013-2016.zip',
                    'pathPontos' : '../data/pontuacao.xlsx',
                    'pathUFCG' : '../../data/external/SERVIDORES_UFCG.xls',
                    'pathSAAP' : '../../data/external/SAAP_UFCG.xls'}
        return kwDefault

    def validaPath(self):
        """Verifica se todos os caminhos informados são válidos e define os booleanos __UFCG, __SAAP e __PONTUA.

        Returns:
            type: Dicionário com caminhos validados.

        """

        kwargs = self.defaultValues()
        kwargs = {**kwargs, **self.kwargs}
        dicio = {k: v for k, v in kwargs.items() if k.startswith('path')}
        for key in list(dicio.keys()):
            try:
                valueP = pathHandler(dicio[key])
                #----É um arquivo e ele pode ser lido?
                assert isfile(valueP) and access(valueP, R_OK), \
                "Arquivo {} não encontrado ou ilegível".format(valueP)
            except AssertionError as error:
                #LOG.error("Arquivo não encontrado %s",valueP )
                dicio.pop(key, None)
            else:
                pass
                #LOG.info("Arquivo encontrado: %s", valueP)
            finally:
                if ('pathSAAP' in dicio and dicio['pathSAAP']!=""):
                    self.__SAAP = True
                else:
                    self.__SAAP = False
                if ('pathUFCG' in dicio and dicio['pathUFCG']!=""):
                    self.__UFCG = True
                else:
                    self.__UFCG = False
                if ('pathPontos' in dicio and dicio['pathPontos']!=""):
                    self.__PONTUA = True
                else:
                    self.__PONTUA = False

        return dicio

    @staticmethod
    def validaISSN_ISBN(*args):
        """Valida lista de ISSNs ou ISBNs.

        Args:
            *args (type): Lista de 1 nível. `*args`.

        Returns:
            type: Lista com ISSN, ISBN ou Invalido, conforme o caso.

        """
        result=[]
        for arg in args:
            try:
                assert (issn.is_valid(arg) or isbn.is_valid(arg)), 'INVALIDO'
                #----issn retorna fora da forma padrão. Retorna o traço
                result.append(arg[:4]+'-'+arg[4:])
            except AssertionError as error:
                result.append('INVALIDO')

        return result

    @staticmethod
    def validaDoi(*args):
        """Valida código DOI na base de dados crossref

        Args:
            *args (type): Lista de um nível com números doi `*args`.

        Returns:
            type: Retorna lista com ISSN se encontrado, NAO-ENCONTRADO ou INVALIDO.

        """
        #----Problemas por aqui. A consulta retorna uma lista.
        works = Works()
        result = []
        args = [*args]
        for arg in args:
            if len(arg) != 0:
                work = Works()
                try:
                    query = work.doi(arg)
                    ISSN = query['ISSN']
                    if ISSN is not None:
                        result.append(query['ISSN'])
                    else:
                        result.append('NAO_ENCONTRADO')
                except CrossrefAPIError as error:
                    result.append('INVALIDO')
            else:
                result.append('INVALIDO')
        return result

    #--------------------------------------------------
    #----Carregando dados externos auxiliares
    def carregaDadosGlobais(self):
        """Carrega todas as tabelas necessárias.

        Returns:
            type: None.

        """
        #----Carrega os dados externos em dataframes uma única vez para ser utililizado.
        #----Alguns arquivos precisam de trabalho adicional
        paths = self.validaPath()
        if self.__PONTUA:
            self.Pontos = pd.read_excel(paths['pathPontos'])
        if self.__SAAP:
            self.SAAP = pd.read_excel(paths['pathSAAP'])

        try:
            self.CAPES = pd.read_csv(paths['pathCAPES'])
            #----Dados da tabela CAPES precisam ser normalizados, sem acentos para comparação de textos.
            self.CAPES = self.CAPES.applymap(unidecode)
        except IOError:
            #LOG.error("Não foi encontrada: Tabela de TABELA_EQUIVALENCIA_CNPQ_CAPES")
            self.CAPES = []

        try:
            #----encoding do CNPq precisa evoluir
            self.QUALIS = pd.read_csv(paths['pathQualis'], encoding = "ISO-8859-1", index_col=0)
            self.QUALIS = self.QUALIS.reset_index()
            #----Coluna de areas tem muitos whitespaces escondidos. stripped.
            self.QUALIS["area"] = self.QUALIS["area"].apply(lambda val: val.strip())
            #----Normaliza e codifica para comparar áreas
            self.QUALIS['area'] = self.QUALIS['area'].str.normalize('NFKD').str.encode('ISO-8859-1', 'ignore')
            #----Este arquivo é muito grande e muitas comparações vão ser feitas. Categorias nas áreas vão agilizar o processamento.
            self.QUALIS['area'] = self.QUALIS['area'].astype('category')
        except pd.errors.EmptyDataError as error:
            tmp = None
            return

        return

    def getFileFromNome(self):
        """Se classe for iniciada por nome, busca nome de arquivo em base de dados externa.

        Returns:
            type: Nome do arquivo.

        """
        if self.__nome is not None:
            #----Se o nome foi informado, coloca na forma canônica
            nome = self.__nome
            nome = nome.upper()
            nome = unidecode(nome)
            if self.__file is not None:
                #----Informando Arquivo e Nome pode ser incompatível. Esqueça o nome.
                #LOG.warning("Conflito NOME-FILE, prevalece FILE")
                return
            else:
                df = pd.read_excel(pathHandler("../../data/external/DBnomes.xlsx"))
                self.__file = df.loc[df['NOME']==nome, 'FILE'].values[0]
        else:
            if self.__file is not None:
                return
            else:
                #LOG.error("Nenhum nome ou arquivo definidos")
                # Não é possível continuar.
                sys.exit(-1)
        return self.__file

    def getDadosUFCG(self, CPF):
        """Caso seja desejada informação da base de dados da SRH, obtem SIAPE e lotação do servidor a partir do CPF.

        Args:
            CPF (type): str `CPF`.

        Returns:
            type: Array [Matrícula, Lotação].

        """
        if self.__UFCG:
            paths = self.validaPath()
            df = pd.read_excel(paths['pathUFCG'])
            df['CPF'] = df['CPF'].astype(str)
            #----Se não encontrar retorna empty dataframe que pode ser manipulado.
            DadosUFCG = df[df['CPF']==CPF][['CPF','Matrícula','Lotação']]
        else:
            DadosUFCG = None
        return DadosUFCG

    def getDadosBasicos(self):
        """Le arquivo XML, inicia parsing e define alguns atributos importantes.

        Returns:
            type: Retorna FLAG de sucesso para ser utilizado nos métodos seguintes.

        """
        file = self.__file
        try:
            #----XML funciona?
            tree = etree.parse(file)
            self.__FLAG = True
        except etree.XMLSyntaxError:
            #LOG.error("XML inválido: %s", file)
            #---- __FLAG impede a determinação de todos os elementos do XML.
            self.__FLAG = False
        except (ParserError, ParseError) as error:
            #LOG.error("XML inválido: %s", file)
            self.__FLAG = False
        else:
            #----Se estes paramêtros não puderem ser definidos o XML não é CV Lattes ou
            #----foi extraído sem informações pessoais.
            self.root = tree.getroot()
            self.ID = self.root.get('NUMERO-IDENTIFICADOR')
            self.Atualiza = self.root.get('DATA-ATUALIZACAO')
            self.NOME = self.root.getchildren()[0].get('NOME-COMPLETO')
            #---- Start Logging
            #LOG.info("cvP:%s", self.NOME)
            if not hasattr(self,'root'):
                #----Sem root nã tem como fazer parsing. Os métodps seguintes retornam None.
                self.__FLAG = False
            if not hasattr(self,'ID'):
                #----Sem ID no xml não podemos prosseguir
                self.__FLAG = False
        return self.__FLAG
    #--------------------------------------------------
    #----Helpers
    def getArea(self):
        """Acessa XML e extrai lista com áreas de conhecimento do perfil do pesquisador.

        Returns:
            type: Lista de AREAS ou None

        """

        #----Temos XML válido?
        if self.__FLAG:
            #----Areas registradas nos dados-gerais do pesquisador
            AREAS = [ el.get("NOME-DA-AREA-DO-CONHECIMENTO") for el in self.root.xpath("DADOS-GERAIS/AREAS-DE-ATUACAO/*")]
            #----tudo maiúsculo. mesma forma da tabela do qualis
            AREAS = [val.upper() for val in AREAS]
            #----elimina duplicatas de um jeito legal
            AREAS = list(OrderedDict.fromkeys(AREAS))
            return AREAS
        else:
            return None

    def getAreaCAPES(self, AREAS):
        """Recebe uma lista de Áreas CNPq e retorna as áreas CAPES equivalentes.

        Args:
            AREAS (type): Areas do CNPq `AREAS`.

        Returns:
            type: Áreas CAPES.

        """
        #----Lista de áreas normalizadas
        AREAS = [*AREAS]
        AREAS = [ val.upper() for val in map(unidecode,AREAS)]
        tabelaCapes = self.CAPES
        #----Retorna as áreas CAPES normalizadas
        capes = tabelaCapes[tabelaCapes["AREA_CNPQ"].isin(AREAS) ]["AREA_CAPES"].str.normalize('NFKD').str.encode('utf-8', 'ignore')

        return capes.tolist()

    def setQualis(self, Area, ISSN):
        """Recebe uma lista de áreas CAPES e um ISSN e devolve qualis.

        Args:
            Area (type): AREA CAPES `Area`.
            ISSN (type): `ISSN`.

        Returns:
            type: Description of returned object.

        """
        #----Várias áreas, um ISSN, tem que atuar no eixo 0 dos df.
        AREAS = [*Area]
        ISSN = ISSN
        dfQUALIS = self.QUALIS[self.QUALIS['area'].isin(AREAS)]
        rank = dfQUALIS[(dfQUALIS['issn']==ISSN)]['ranking'].tolist()
        if len(rank)>0:
            rank = sorted(rank)[0]
        else:
            rank = ''
        return rank

    @staticmethod
    def BlocoLattes(root, lista ):
        """Método estático para parse de uma raiz XML com profundidade específica comum a várias seções do Lattes

        Args:
            root (type): raiz do XML `root`.
            lista (type): Contem uma TAG principal e tags na outra hierarquia `lista`.

        Returns:
            type: Description of returned object.

        """
        raiz = root
        TAG = lista[0]
        bloco = lista[1]
        lista = [ {'PRODUCAO':el3.tag, **el3.attrib,  **el4.attrib}
        for el1 in raiz.iterchildren(tag=TAG)
        for el2 in el1.iterchildren()
        for el3 in el2.iterchildren()
        for el4 in el3.iterchildren() if any(tag in el4.tag for tag in [bloco]) ]

        return lista

    def xml2dict(self, tag=""):
        """A partir do XML, extrai as informações para um dicionário.

        Args:
            tag (type): um dos tipos de produções desejados `tag`. Defaults to "".

        Returns:
            type: duas listas com as informações de dicionários.

        """
        tag = tag
        listaProducoes = ['PRODUCAO-BIBLIOGRAFICA', 'PRODUCAO-TECNICA', 'OUTRA-PRODUCAO','DADOS-COMPLEMENTARES']
        if self.__FLAG:
            if 'DADOS-GERAIS' in tag:
                tree = self.root.iterchildren(tag='DADOS-GERAIS')
                lista_dados = [{**el1.attrib} for el1 in tree]
                lista_detalhe = []
            elif "FORMACAO-ACADEMICA-TITULACAO" in tag:
                tree = self.root.iterchildren(tag='DADOS-GERAIS')
                lista_dados = [{'TITULACAO':el3.tag, **el3.attrib}
                    for el1 in tree
                    for el2 in el1.iterchildren(tag=tag)
                    for el3 in el2.iterchildren()]
                lista_detalhe = []
            elif tag in listaProducoes:
                lista_dados = self.BlocoLattes(root = self.root, lista=[tag, 'DADOS'])
                if tag in ['PRODUCAO-BIBLIOGRAFICA', 'OUTRA-PRODUCAO']:
                    lista_detalhe = self.BlocoLattes(root = self.root, lista = [tag, 'DETALHAMENTO'])
                elif 'PRODUCAO-TECNICA' in tag:
                    lista_detalhe = [{'PRODUCAO':el3.tag, **el3.attrib,  'FOMENTO': el4['INSTITUICAO-FINANCIADORA']}
                     for el1 in self.root.iterchildren(tag=tag)
                     for el2 in el1.iterchildren()
                     for el3 in el2.iterchildren()
                     for el4 in el3.iterchildren()
                         if (any(tag in el4.tag for tag in ["DETALHAMENTO"]) and ['INSTITUICAO-FINANCIADORA'] in el4.keys()) ]
                else:
                    lista_detalhe = []
            else:
                print('TAG:',tag,'invalida')

            lista = [lista_dados,lista_detalhe]
        else:
            lista = None

        return lista

    def xml2dict_3(self, tag, tipo):
        if self.__FLAG:
            lista = [{'PRODUCAO':el2.tag, **el2.attrib, 'TIPO-PRODUCAO':el3.tag, **el3.attrib}
                            for el1 in self.root.iterchildren(tag=tag)
                            for el2 in el1.iterchildren()
                            for el3 in el2.iterchildren()
                                if any(tag in el3.tag for tag in tipo)]
        else:
            lista = None
        return lista

    def dfTidy(self, df, cols_keep, cols_merge, cols_equiv, cols_out, cols_final):
        """Trabalha as colunas nos dataframes de cada elemento Lattes para ficarem mais amigaveis.

        Args:
            df (type):  dataframe com informações de uma seção `df`.
            cols_keep (type): lista de parâmetros para trabalhar `cols_keep`.
            cols_merge (type): parâmetros que serão unificados porque aparecem com nomes diferentes `cols_merge`.
            cols_equiv (type): mudanças de nome de tabelas como dicionário `cols_equiv`.
            cols_out (type): colunas para remover, incluindo substrings `cols_out`.
            cols_final (type): colunas para tentar manter `cols_final`.

        Returns:
            type: dataframe mais ou menos arrumado.

        """
        #----Validando argumentos
        if (not isinstance(df,pd.DataFrame) or df.empty):
            return None
        #----
        cols = df.columns.tolist()
        if len(cols_keep)==0:
            cols_keep = cols
        if len(cols_merge)==0:
            cols_merge = None
        if len(cols_equiv)==0:
            cols_equiv = None
        if len(cols_out)==0:
            cols_out = None
        #-------------------------
        #----Sabemos o que manter, garantimos que exista
        if cols_keep is not None:
            cols = df.columns.tolist()
            colG = list(OrderedDict.fromkeys([*cols_keep, *cols]))
            for col in colG :
                if col not in cols:
                    df.loc[:,col] = ""
        #----Removendo tudo o que parece inútil
        if cols_out is not None:
            present = {col for col in set(cols) if any(out in col for out in set(cols_out))}
            cols = list(set(cols) - present)
            df = df[cols]
        #----Renomeando algumas colunas
        if cols_equiv is not None:
            for col in cols_equiv.keys():
                if col not in cols:
                    df = df.copy()
                    df.loc[:,col] = ""
            df = df.rename(columns = cols_equiv)
        #----Juntando colunas que representam a mesma coisa com fillna
        cols = sorted(df.columns.tolist())
        cols_merge = sorted(cols_merge)
        if cols_merge is not None:
            for agregado in cols_merge:
                colmerge = sorted((([col for col in cols if agregado in col]))+[agregado])
                colmerge = list(OrderedDict.fromkeys(colmerge))
                for col in colmerge:
                    if col not in cols:
                        df.loc[:,col]=np.nan
                for col in colmerge[1:]:
                    if not df[col].isnull().all():
                        df = df.copy()
                        df.loc[:,colmerge[0]] = df.loc[:,colmerge[0]].fillna(df.loc[:,col])
                        df = df.drop(col, axis=1)

        cols = df.columns.tolist()
        if len(cols_final) == 0:
            cols_final = cols
        colunas = list({col for col in set(cols) if any(out in col for out in set(cols_final))})

        df = df[colunas]

        return df

    @staticmethod
    def fixDF(df, cols_fix):
        if df is not None:
            cols = df.columns.to_list()
            for col in cols_fix:
                if col not in cols:
                    df[col] = ''
            df = df[cols_fix]
            df = df.fillna('')
        return df

    #--------------------------------------------------
    #----Informações do CV Lattes
    def getDadosPessoais(self):
        """Extrai informações pessoais do pesquisador do XML do Lattes. Precisa da definição da raiz do XML que é realizada em getDadosBasicos.

        Returns:
            type: Dataframe.

        """
        #----Se existe um Lattes
        if self.__FLAG:
            dictPessoais = {}
            #----Parse Data, xml2dict returns double list, only first is relevant here
            lista = self.xml2dict(tag = 'DADOS-GERAIS')
            df = pd.DataFrame(lista[0])
            #---- Organizando DF
            cols = ["NOME-COMPLETO", "CPF", "PAIS-DE-NASCIMENTO", "UF-NASCIMENTO", "DATA-NASCIMENTO","SEXO", "RACA-OU-COR"]
            cols = df.columns.tolist()
            new_cols = list(set(cols).intersection(set(cols)))
            df = df[new_cols]
            #----
            #----Incluindo dados do bando de servidores
            if self.__UFCG:
                try:
                    CPF = df['CPF'].values[0]
                except KeyError as error:
                    #LOG.warning("Não esta na lista de servidores %s", self.ID)
                    tmp = None
                else:
                    data_UFCG = self.getDadosUFCG(CPF)
                    df = df.merge(data_UFCG, on='CPF', how='left')
            #---- Inclui Identificador Lattes
            df["ID"] = self.ID
            df["DATA-ATUALIZACAO"] = self.Atualiza
            try:
                IES = list(self.root.findall("DADOS-GERAIS/ENDERECO/ENDERECO-PROFISSIONAL"))[0].attrib['NOME-INSTITUICAO-EMPRESA']
            except IndexError as error:
                IES = 'NAO ENCONTRADO'
            df["IES"] = IES
            result = df
        else:
            result =  None
        return result

    def getDadosTitulacao(self):
        """Extrai informações de Titulacao do pesquisador do XML do Lattes. Precisa da definição da raiz do XML que é realizada em getDadosBasicos.

        Returns:
                type: Dataframe.

        """
        if self.__FLAG:
            #----Ajustando informações para o bloco
            TAGs = 'FORMACAO-ACADEMICA-TITULACAO'
            #----Definindo quais campos são úteis
            #--------Colunas com múltiplas equivalências.
            cols_merge = ['TIPO', 'NOME-INSTITUICAO']
            #--------Colunas que tem que mudar de nome
            cols_equiv = {}
            #--------Juntando todos os dados básicos e complementares.
            cols_keep = []
            #--------Jogue fora toda coluna com estes termos
            cols_out = ['INGLES', 'CODIGO', 'FLAG', 'ORIENTADOR', 'OUTRA', 'TITULO']
            cols_final = []
            #----Parsing
            lista = self.xml2dict(tag = TAGs)
            df = df = pd.DataFrame(lista[0])
            if not df.empty:
                #----Tidying up
                df = self.dfTidy(df, cols_keep=cols_keep, cols_merge = cols_merge, cols_equiv = cols_equiv, cols_out=cols_out, cols_final=cols_final)

                result =  df
            else:
                result = None
        else:
            result = None
        return  result

    def getProducaoBibliografica(self):
        """Extrai informações de Producao Bibliografica do pesquisador do XML do Lattes. Insere informação de ISSN e ISBN, verifica validade e ajusta fator QUALIS. Precisa da definição da raiz do XML que é realizada em getDadosBasicos.

        Returns:
                type: Dataframe.

        """
        if self.__FLAG:
            #----Ajustando informações para o bloco
            TAGs = 'PRODUCAO-BIBLIOGRAFICA'
            #----Definindo quais campos são úteis
            #--------Colunas dos Dados Básicos
            cols_BIB_basicos = ['PRODUCAO', 'TIPO', 'NATUREZA','TITULO', 'ANO', 'PAIS', 'MEIO', 'DOI', 'REVISTA' ]
            #--------Colunas dos dados complementares.
            cols_BIB_complementares = ['CLASSIFICACAO', 'NOME', 'EDITORA', 'ISSN-ISBN', 'ISSN', 'ISBN']
            #--------Colunas com múltiplas equivalências.
            cols_merge = ['ANO', 'TITULO','PAIS', 'REVISTA', 'MEIO', 'ISSN-ISBN']
            #--------Colunas que tem que mudar de nome
            cols_equiv = {'TITULO-DO-PERIODICO-OU-REVISTA':'REVISTA', 'TITULO-DOS-ANAIS-OU-PROCEEDINGS':'REVISTA-PROC', 'TITULO-DO-JORNAL-OU-REVISTA':'REVISTA-JORNAL', 'ISBN':'ISSN-ISBN-1', 'ISSN':'ISSN-ISBN-2'}
            #--------Juntando todos os dados básicos e complementares.
            cols_keep = list(OrderedDict.fromkeys([*cols_BIB_basicos, *cols_BIB_complementares]))
            #--------Jogue fora toda coluna com estes termos
            cols_out = ['INGLES', 'CODIGO', 'FLAG', 'HOME', 'CIDADE','PAGINA']
            cols_final = ['SEQUENCIA-PRODUCAO', 'PRODUCAO', 'NATUREZA', 'CLASSIFICACAO', 'TIPO', 'TITULO', 'ANO', 'PAIS', 'REVISTA','DOI', 'ISBN' ,'NOME']
            #----Parsing
            lista = self.xml2dict(tag = TAGs)
            df = pd.concat([pd.DataFrame(lista[0]), pd.DataFrame(lista[1])], ignore_index=True)
            if not df.empty:
                #----Tidying up
                df = self.dfTidy(df, cols_keep=cols_keep, cols_merge = cols_merge, cols_equiv = cols_equiv, cols_out=cols_out, cols_final=cols_final)
                #----Filtrando
                df = df[df['ANO'].isin(self.periodo)]
                #----Validando ISSN/ISBN
                if 'ISSN-ISBN' not in df.columns.tolist():
                    if 'ISSN' not in df.columns.tolist():
                        if 'ISBN' in df.columns.tolist():
                            df.rename(columns = {'ISBN':'ISSN-ISBN'})
                        else:
                            df['ISSN-ISBN'] = ""
                    else:
                        df.rename(columns = {'ISSN':'ISSN-ISBN'})
                df['ISSN-ISBN'] = df['ISSN-ISBN'].apply(self.validaISSN_ISBN)
                df['ISSN-ISBN'] = df['ISSN-ISBN'].apply(lambda val: ''.join(val))
                #---- QUALIS
                AREA_CNPQ = self.getArea()
                AREA_CAPES = self.getAreaCAPES(AREA_CNPQ)
                try:
                    df['QUALIS'] = df.apply(lambda val: self.setQualis(AREA_CAPES, val['ISSN-ISBN']) if val['ISSN-ISBN']!='INVALIDO' else '', axis = 1)
                except ValueError as error:
                    df['QUALIS'] = 'ERRO'
                #----doi
                #DOI = self.validaDoi(df.DOI)
                df["ID"] = self.ID
                result =  df
            else:
                result = None
        else:
            result = None
        return result

    def getProducaoTecnica(self):
        """Extrai informações de Producao Tecnica do pesquisador do XML do Lattes.

        Returns:
                type: Dataframe.

        """
        if self.__FLAG:
            #----Ajustando informações para o bloco
            TAGs = 'PRODUCAO-TECNICA'
            #----Definindo quais campos são úteis
            #--------Colunas com múltiplas equivalências.
            cols_merge = ['ANO', 'TITULO','PAIS']
            #--------Colunas que tem que mudar de nome
            cols_equiv = {'INSTITUICAO-FINANCIADORA':'FOMENTO'}
            #--------Juntando todos os dados básicos e complementares.
            cols_keep = ['ANO','PRODUCAO', 'SEQUENCIA-PRODUCAO', 'TIPO-PRODUCAO', 'NATUREZA', 'PAIS', 'IDIOMA', 'DOI', 'FINALIDADE', 'INSTITUICAO-FINANCIADORA','TITULO', 'NOME-COMPLETO-DO-AUTOR', 'CATEGORIA', 'TIPO-PRODUTO']
            #--------Jogue fora toda coluna com estes termos
            cols_out = ['INGLES', 'CODIGO', 'FLAG', 'HOME', 'CIDADE','PAGINA']
            #--------
            cols_final = ['ANO','PRODUCAO', 'SEQUENCIA-PRODUCAO', 'TIPO-PRODUCAO', 'NATUREZA', 'PAIS', 'IDIOMA', 'DOI', 'FINALIDADE', 'INSTITUICAO-FINANCIADORA','TITULO', 'NOME-COMPLETO-DO-AUTOR', 'CATEGORIA', 'TIPO-PRODUTO']
            #----Parsing
            TIPO_PRODUCAO = ['DADOS-BASICOS-DO-SOFTWARE', 'DADOS-BASICOS-DA-PATENTE','APRESENTACAO-DE-TRABALHO', 'ORGANIZACAO-DE-EVENTO', 'DADOS-BASICOS-DO-TRABALHO-TECNICO', 'CURSO-DE-CURTA-DURACAO-MINISTRADO', 'PROGRAMA-DE-RADIO-OU-TV', 'RELATORIO-DE-PESQUISA', 'OUTRA-PRODUCAO-TECNICA', 'EDITORACAO', 'DADOS-BASICOS-DO-PROCESSOS-OU-TECNICAS', 'DADOS-BASICOS-DO-PRODUTO-TECNOLOGICO', 'DESENVOLVIMENTO-DE-MATERIAL-DIDATICO-OU-INSTRUCIONAL', 'MIDIA-SOCIAL-WEBSITE-BLOG', 'DADOS-BASICOS-DA-MARCA', 'CARTA-MAPA-OU-SIMILAR', 'MAQUETE']
            lista = self.xml2dict_3(tag = TAGs, tipo = TIPO_PRODUCAO )
            df = pd.DataFrame(lista)
            if not df.empty:
                #----Tidying up
                df = self.dfTidy(df, cols_keep=cols_keep, cols_merge = cols_merge, cols_equiv = cols_equiv, cols_out=cols_out, cols_final=cols_final)
                #----Filtrando
                df = df[df['ANO'].isin(self.periodo)]
                df["ID"] = self.ID
                result =  df
            else:
                result = None
        else:
            result =  None
        return result

    def getApresentacoes(self):
        """Obtem as informações da seção OUTRA-PRODUCAO do currículo do pesquisador

        Returns:
            type: Dataframe.

        """
        if self.__FLAG:
            #----Ajustando informações para o bloco
            TAGs = 'PRODUCAO-TECNICA'
            #----Definindo quais campos são úteis
            #--------Colunas com múltiplas equivalências.
            cols_merge = ['ANO', 'TITULO','PAIS']
            #--------Colunas que tem que mudar de nome
            cols_equiv = {}
            #--------Juntando todos os dados básicos e complementares.
            cols_keep =  ['PRODUCAO', 'SEQUENCIA-PRODUCAO', 'NATUREZA', 'TITULO', 'ANO', 'PAIS', 'IDIOMA', 'DOI','PRODUCAO', 'SEQUENCIA-PRODUCAO', 'TIPO-DE-ORIENTACAO', 'NOME-DO-ORIENTANDO', 'NOME-DA-AGENCIA']
            #--------Jogue fora toda coluna com estes termos
            cols_out = ['INGLES', 'CODIGO', 'FLAG', 'HOME', 'CIDADE','PAGINA']
            cols_final = ['SEQUENCIA-PRODUCAO', 'PRODUCAO', 'TIPO','NATUREZA', 'TITULO','ANO', 'PAIS', 'IDIOMA', 'DOI']
            #----Parsing
            lista = self.xml2dict(tag = TAGs)
            df = pd.concat([pd.DataFrame(lista[0]), pd.DataFrame(lista[1])], ignore_index=True)
            if not df.empty:
                #----Tidying up
                df = self.dfTidy(df, cols_keep=cols_keep, cols_merge = cols_merge, cols_equiv = cols_equiv, cols_out=cols_out, cols_final=cols_final)
                #----Filtrando
                df = df[df['ANO'].isin(self.periodo)]
                df["ID"] = self.ID
                result = df
            else:
                result = None
        else:
            result = None
        return result

    def getProducaoOutra(self):
        """Obtem as informações da seção OUTRA-PRODUCAO do currículo do pesquisador

        Returns:
            type: Dataframe.

        """
        if self.__FLAG:
            #----Ajustando informações para o bloco
            TAGs = 'OUTRA-PRODUCAO'
            #----Definindo quais campos são úteis
            #--------Colunas com múltiplas equivalências.
            cols_merge = ['ANO', 'TITULO','PAIS']
            #--------Colunas que tem que mudar de nome
            cols_equiv = {}
            #--------Juntando todos os dados básicos e complementares.
            cols_keep =  ['PRODUCAO', 'SEQUENCIA-PRODUCAO', 'NATUREZA', 'TITULO', 'ANO', 'PAIS', 'IDIOMA', 'DOI','PRODUCAO', 'SEQUENCIA-PRODUCAO', 'TIPO-DE-ORIENTACAO', 'NOME-DO-ORIENTANDO', 'NOME-DA-AGENCIA']
            #--------Jogue fora toda coluna com estes termos
            cols_out = ['INGLES', 'CODIGO', 'FLAG', 'HOME', 'CIDADE','PAGINA']
            cols_final = ['SEQUENCIA-PRODUCAO', 'PRODUCAO', 'TIPO','NATUREZA', 'TITULO','ANO', 'PAIS', 'IDIOMA', 'DOI']
            #----Parsing
            lista = self.xml2dict(tag = TAGs)
            df = pd.concat([pd.DataFrame(lista[0]), pd.DataFrame(lista[1])], ignore_index=True)
            if not df.empty:
                #----Tidying up
                df = self.dfTidy(df, cols_keep=cols_keep, cols_merge = cols_merge, cols_equiv = cols_equiv, cols_out=cols_out, cols_final=cols_final)
                #----Filtrando
                df = df[df['ANO'].isin(self.periodo)]
                df["ID"] = self.ID
                result = df
            else:
                result = None
        else:
            result = None
        return result

    def getDadosComplementares(self):
        """Extrai informações de DADOS-COMPLEMENTARES do pesquisador do XML do Lattes.

        Returns:
                type: Dataframe.

        """
        if self.__FLAG:
            #----Ajustando informações para o bloco
            TAGs = 'DADOS-COMPLEMENTARES'
            #----Definindo quais campos são úteis
            #--------Colunas com múltiplas equivalências.
            cols_merge = ['ANO', 'TITULO','PAIS']
            #--------Colunas que tem que mudar de nome
            cols_equiv = {}
            #--------Juntando todos os dados básicos e complementares.
            cols_keep =  ['PRODUCAO', 'SEQUENCIA-PRODUCAO', 'NATUREZA', 'TITULO', 'ANO', 'PAIS', 'IDIOMA', 'TIPO-PARTICIPACAO']
            #--------Jogue fora toda coluna com estes termos
            cols_out = ['INGLES', 'CODIGO', 'FLAG', 'HOME', 'CIDADE','PAGINA']
            cols_final = ['SEQUENCIA-PRODUCAO', 'PRODUCAO', 'TIPO','NATUREZA', 'TITULO','ANO', 'PAIS', 'IDIOMA', 'DOI']
            #----Parsing
            lista = self.xml2dict(tag = TAGs)
            df = pd.DataFrame(lista[0])
            #----Tidying up
            if ((not df.empty) and (df is not None)) :
                df = self.dfTidy(df, cols_keep=cols_keep, cols_merge = cols_merge, cols_equiv = cols_equiv, cols_out=cols_out, cols_final=cols_final)
                #----Filtrando
                df = df[df['ANO'].isin(self.periodo)]

                result = df
            else:
                result = None
        else:
            result = None
        return result

    def getDadosProjetos(self):
        """TODO.

        Returns:
            type: Description of returned object.

        """
        # Extrai Participação de Projetos
        for el1 in CVparse.root.iterchildren(tag='DADOS-GERAIS'):
            for el2 in el1.iterchildren(tag='ATUACOES-PROFISSIONAIS'):
                for el3 in el2.iterchildren():
                    for el4 in el3.iterchildren(tag = 'ATIVIDADES-DE-PARTICIPACAO-EM-PROJETO'):
                        for el5 in el4.iterchildren():
                            for el6 in el5.iterchildren():
                                    [el5.tag, el6.tag, el6.attrib]
        return
    #--------------------------------------------------
    #----Resumos e Relatórios
    def doSumario(self):
        """Chamada no jupyter notebook para gerar relatório geral do pesquisador.

        Returns:
            type: Dicionário com elementos do Sistema, Sumários e dados listados de produção.

        """

        #----Arquivos externos utilizados
        tmp = self.validaPath();
        dfExternos = pd.DataFrame([tmp])
        dadosGlobais = None
        Pessoais = None
        Titulacao = None
        Bibliografico = None
        Apresentacoes = None
        Tecnico = None
        Outra = None
        Complementares = None

        dadosGlobais = self.carregaDadosGlobais()
        Pessoais = self.getDadosPessoais()
        Titulacao = self.getDadosTitulacao()
        Bibliografico = self.getProducaoBibliografica()
        Apresentacoes = self.getApresentacoes()
        Tecnico = self.getProducaoTecnica()
        Outra = self.getProducaoOutra()
        Complementares = self.getDadosComplementares()
        #----Colunas
        cols_final_Pessoais = ['DATA-ATUALIZACAO', 'ID', 'CPF', 'Matrícula', 'IES', 'Lotação', 'NOME-COMPLETO']
        cols_final_Demograficos = ['NOME-COMPLETO', 'DATA-NASCIMENTO','NACIONALIDADE', 'RACA-OU-COR', 'SEXO', 'UF-NASCIMENTO']
        colsP = cols_final_Pessoais + cols_final_Demograficos
        for col in colsP:
            if col not in Pessoais.columns.tolist():
                Pessoais.loc[:,col] = ''
        #----Titulos
        cols_final_Titulos = ['ANO-DE-CONCLUSAO','ANO-DE-INICIO','TITULACAO', 'NOME-INSTITUICAO', 'NOME-AGENCIA', 'TIPO',
                              'SEQUENCIA-FORMACAO','NOME-CURSO', 'STATUS-DO-CURSO']
        #----Bibliograficos
        lista_final_Bib = ['ANO', 'SEQUENCIA-PRODUCAO', 'PRODUCAO', 'NATUREZA', 'CLASSIFICACAO-DO-EVENTO', 'NOME-DO-EVENTO', 'TITULO', 'PAIS','REVISTA', 'DOI', 'ISSN-ISBN', 'QUALIS']
        #----Tecnicos
        lista_final_Tec = ['ANO', 'SEQUENCIA-PRODUCAO', 'PRODUCAO', 'NATUREZA','TIPO-PRODUCAO','TITULO']
        #----Apresentacoes
        lista_final_Apresentacoes = ['ANO', 'SEQUENCIA-PRODUCAO', 'PRODUCAO', 'NATUREZA','TITULO', 'IDIOMA', 'PAIS', 'DOI']
        #----Outras
        lista_final_Outra = ['ANO', 'SEQUENCIA-PRODUCAO', 'PRODUCAO', 'NATUREZA', 'TIPO-DE-ORIENTACAO-CONCLUIDA','TITULO',
                             'IDIOMA', 'PAIS']
        #----Complementares
        lista_final_Complementares = ['ANO', 'SEQUENCIA-PRODUCAO', 'PRODUCAO', 'NATUREZA', 'TITULO', 'IDIOMA', 'PAIS', 'DOI']
        #----Arrumando dataframes
        #----
        Pessoal = Pessoais[cols_final_Pessoais]
        Demografico = Pessoais[cols_final_Demograficos]
        Titulacao = Titulacao[cols_final_Titulos]
        Bibliografico = Bibliografico[lista_final_Bib]
        Bibliografico = Bibliografico.fillna('')
        Bibliografico = Bibliografico.assign(flag_ISSN = lambda val: True if (len(val['ISSN-ISBN'])>8 or len(val['DOI'])>0) else '', flag_Qualis = lambda val: True if (str(val['QUALIS'])[0] in ['A','B']) else '')
        #----
        Tecnico = self.fixDF(Tecnico, lista_final_Tec)
        #----
        Apresentacoes = self.fixDF(Apresentacoes, lista_final_Apresentacoes)
        #----
        Outra = self.fixDF(Outra, lista_final_Outra)
        #----
        Complementares = self.fixDF(Complementares, lista_final_Complementares)
        #----
        #----Resumos
        if Bibliografico is not None:
            resumo_Biblio = Bibliografico.groupby(['PRODUCAO', 'NATUREZA',
                                                   'CLASSIFICACAO-DO-EVENTO', 'flag_Qualis','flag_ISSN' ,
                                                   'PAIS'])['SEQUENCIA-PRODUCAO'].count().reset_index()
            resumo_Biblio = resumo_Biblio.rename(columns={'CLASSIFICACAO-DO-EVENTO':'TIPO'})
        #----
        if Tecnico is not None:
            resumo_Tec = Tecnico.groupby(['PRODUCAO','NATUREZA', 'TIPO-PRODUCAO'])['SEQUENCIA-PRODUCAO'].count().reset_index()
            resumo_Tec = resumo_Tec.rename(columns={'TIPO_PRODUCAO':'TIPO'})
        #----
        if Apresentacoes is not None:
            resumo_Apres = Apresentacoes.groupby(['PRODUCAO', 'NATUREZA','PAIS'])['SEQUENCIA-PRODUCAO'].count().reset_index()
        #----
        if Outra is not None:
            resumo_Outra = Outra.groupby(['PRODUCAO', 'NATUREZA','PAIS', 'TIPO-DE-ORIENTACAO-CONCLUIDA'])['SEQUENCIA-PRODUCAO'].count().reset_index()
            resumo_Outra = resumo_Outra.rename(columns={'TIPO-DE-ORIENTACAO-CONCLUIDA':'TIPO'})
        #----
        if Complementares is not None:
            resumo_Comp = Complementares.groupby(['PRODUCAO', 'NATUREZA','PAIS'])['SEQUENCIA-PRODUCAO'].count().reset_index()
            #----

        resultado = {'DB':[dfExternos],
                     'RESUMOS':[Pessoal, Demografico, Titulacao, resumo_Biblio, resumo_Apres, resumo_Tec, resumo_Outra, resumo_Comp],
                     'PRODUCAO':[Bibliografico, Apresentacoes, Tecnico, Outra, Complementares]}
        return resultado

    def doPontuacao(self, save_to_disk=True):

        #----Roda os métodos d classe para coletar as informações necessárias
        ano_doutor = 0
        Pessoais = self.getDadosPessoais()
        Titulos = self.getDadosTitulacao()
        Bib = self.getProducaoBibliografica()
        Tec = self.getProducaoTecnica()
        Outra = self.getProducaoOutra()
        Area = ' '.join(self.getArea())
        doutorBoole = Titulos[Titulos['TITULACAO']=='DOUTORADO']['ANO-DE-CONCLUSAO']
        if not doutorBoole.empty:
            try:
                ano_doutor = int(Titulos[Titulos['TITULACAO']=='DOUTORADO']['ANO-DE-CONCLUSAO'].max())
            except (UnboundLocalError, ValueError) as error:
                ano_doutor = 0
        else:
            ano_doutor = 0
        #----Arrumando as informações dos daframes, cortando colunas e cuidando de NA
        #----Bibliográficos
        if Bib is not None:
            lista_final_Bib = ['ID','SEQUENCIA-PRODUCAO', 'PRODUCAO', 'NATUREZA', 'PAIS']
            Bib = Bib[lista_final_Bib]
            Bib = Bib.fillna('')
            #----Cria flag nacional para pontuacao

            Bib = Bib.assign(flag_Nacional = lambda val: 'Nacional' if str(val['PAIS'])=='Brasil' else ('Internacional' if len(val['PAIS']!=0) else " "))
            #----Apenas 3 níveis podem ter diferença de pontuação
            resumo_Biblio = Bib.groupby(['ID','PRODUCAO', 'NATUREZA', 'flag_Nacional'])['SEQUENCIA-PRODUCAO'].nunique().reset_index()
            resumo_Biblio = resumo_Biblio[['ID','PRODUCAO', 'NATUREZA', 'flag_Nacional','SEQUENCIA-PRODUCAO']]
        else:
            resumo_Biblio = pd.DataFrame()
        #----Tecnicos
        if Tec is not None:
            lista_final_Tec = ['ID','SEQUENCIA-PRODUCAO', 'PRODUCAO', 'NATUREZA']
            if 'NATUREZA' not in Tec.columns.tolist():
                Tec['NATUREZA'] = np.nan
            resumo_Tec = Tec.groupby(['ID','PRODUCAO','NATUREZA'])['SEQUENCIA-PRODUCAO'].nunique().reset_index()
            resumo_Tec = resumo_Tec[['ID','PRODUCAO', 'NATUREZA','SEQUENCIA-PRODUCAO']]
        else:
            resumo_Tec = pd.DataFrame()
        #----dfOutra
        if  Outra is not None:
            Outra = Outra.fillna("")
            resumo_Outra = Outra.groupby(['ID','PRODUCAO', 'NATUREZA'])['SEQUENCIA-PRODUCAO'].nunique().reset_index()
            resumo_Outra = resumo_Outra[['ID','PRODUCAO', 'NATUREZA','SEQUENCIA-PRODUCAO']]
        else:
            resumo_Outra = pd.DataFrame()
        #----Concatena
        dflist = [resumo_Biblio, resumo_Tec, resumo_Outra]
        list_df_tipos = [val for val in dflist if not val.empty]
        try:
            df_tipos = pd.concat(list_df_tipos)
        except ValueError as error:
            NOTA_pesquisador = pd.DataFrame()
            NOTA_pesquisador['ID'] = self.ID
        else:
            #----sumários
            if 'NATUREZA' not in df_tipos.columns.tolist():
                df_tipos['NATUREZA'] = np.nan
            if 'flag_Nacional' not in df_tipos.columns.tolist():
                df_tipos['flag_Nacional'] = np.nan
            df_producoes = df_tipos.groupby(['ID', 'PRODUCAO', 'NATUREZA', 'flag_Nacional'])['SEQUENCIA-PRODUCAO'].nunique().reset_index()
            NOTAS_raw = df_producoes.merge(self.Pontos, on=['PRODUCAO', 'NATUREZA', 'flag_Nacional'], how = 'left')
            #----Calcula notas das produções agrupadas
            NOTAS_raw['NOTA_ITEM'] = NOTAS_raw['SEQUENCIA-PRODUCAO']*NOTAS_raw['PONTOS']
            NOTAS_raw['NOTAS_CUT'] = np.where(((NOTAS_raw['NOTA_ITEM']<=NOTAS_raw['MAX']) |
                                       (NOTAS_raw['MAX'].isna())), NOTAS_raw['NOTA_ITEM'], NOTAS_raw['MAX'])
            NOTAS_raw = NOTAS_raw[['ID', 'NOTA_ITEM', 'NOTAS_CUT']]
            #----Sumariza para notas únicas
            NOTA_pesquisador = NOTAS_raw.groupby('ID').sum().reset_index()

        #----Dados Pessoais
        NOTA_pesquisador['NOME'] = self.NOME
        NOTA_pesquisador = NOTA_pesquisador.merge(Pessoais, on ='ID', how='inner')
        #----CPF pode ser complicado
        try:
            CPF = self.root.find('DADOS-GERAIS').attrib['CPF']
        except (KeyError, AttributeError) as error:
            CPF = self.file.split('/')[-1].split('-')[0]
        #NOTA_pesquisador.loc[:,'CPF'] = CPF
        NOTA_pesquisador['CPF'] = CPF
        #----Para fazer análises apenas
        NOTA_pesquisador['AREA'] = " ".join(self.getArea())
        #----Doutorado influi na pontuação
        NOTA_pesquisador['DOUTORADO'] = ano_doutor
        NOTA_Doutorado = 12 if (2020-ano_doutor)<=5 else ( 8 if ano_doutor!=0 else 0)
        #----Participações no SAAP: precisa de informação de CPF!
        if isinstance(CPF,str):
            DATA_saap = self.SAAP
            DATA_saap = self.SAAP[['CPF_NUMERO','AVALIACOES']]
            DATA_saap.columns = ['CPF',"AVALIACOES"]
            DATA_saap = DATA_saap.copy()
            DATA_saap.loc[:,'CPF'] = DATA_saap['CPF'].astype(str)
            DATA_saap.loc[:,'PONTOS'] = DATA_saap['AVALIACOES']*0.25
            n_saap =  max(DATA_saap[DATA_saap['CPF']== CPF]['PONTOS'].values,2) if CPF in DATA_saap['CPF'].tolist() else 0
            NOTA_pesquisador['SAAP'] = n_saap
            NOTA_Doutorado = 12 if (2020-ano_doutor)<=5 else ( 8 if ano_doutor!=0 else 0)
        else:
            n_saap = 0
        if 'NOTA_ITEM' in NOTA_pesquisador.columns.tolist():
            NOTA_pesquisador['SAAP'] = n_saap
            NOTA_pesquisador = NOTA_pesquisador.assign(Nota_CVP = NOTA_Doutorado + max(NOTA_pesquisador['NOTA_ITEM'].values,NOTA_pesquisador['NOTA_ITEM'].values) + n_saap)
        else:
            n_saap = 0
            NOTA_pesquisador['SAAP'] = n_saap
            NOTA_pesquisador['NOTA_ITEM'] = 0
            NOTA_pesquisador['NOTAS_CUT'] = 0
            NOTA_pesquisador = NOTA_pesquisador.assign(Nota_CVP = NOTA_Doutorado + max(NOTA_pesquisador['NOTA_ITEM'].values,NOTA_pesquisador['NOTA_ITEM'].values) + n_saap)

        if 'Nota_ITEM' in NOTA_pesquisador.columns.tolist():
            NOTA_pesquisador['SAAP'] = n_saap
            NOTA_pesquisador['Nota_CVP']=0

        df = NOTA_pesquisador[['AREA', 'ID', 'CPF', 'NOME', 'DOUTORADO','NOTA_ITEM', 'NOTAS_CUT', 'SAAP', 'Nota_CVP']]

        if save_to_disk:
            inicio = dt.now().strftime('%d_%m_%Y__%H_%M')
            name = '../../reports/CVsranking/' + self.ID + '-[' + self.NOME + ']-' + inicio + '.xlsx'
            filename =  pathHandler(name)
            df.to_excel(filename, index = False)
        return df

    def doReportPDF(self):
        return

def doNotasUFCG(path, save_to_disk=True):
    pessoas = pd.read_excel(path)
    CVPs = []
    for cv in pessoas['FILE'].to_list():
        PQ = Pesquisador()
        PQ.periodo = ['2020','2019','2018','2017'];
        PQ.file = cv
        tmp = PQ.validaPath();
        tmp1 = PQ.carregaDadosGlobais()
        tmp2 = PQ.getDadosBasicos();
        CVPs.append(PQ.doPontuacao(save_to_disk = save_to_disk))
    NOTAS_UFCG = pd.concat(CVPs)
    if save_to_disk:
        inicio = dt.now().strftime('%d_%m_%Y__%H_%M')
        name = '../../reports/CVsranking/' + 'NOTAS_UFCG_' + inicio + '.xlsx'
        filename =  pathHandler(name)
        NOTAS_UFCG.to_excel(filename, index = False)
        NOTAS_UFCG.to_excel()

    return NOTAS_UFCG
#-----------------------------------------------------------------------
def main():
    PATH = pathHandler()
    sys.path.append(PATH)
    #LOG.info("Rodando o módulo: %s",PATH)
    pass

if __name__ == "__main__":
    main()
#-----------------------------------------------------------------------
