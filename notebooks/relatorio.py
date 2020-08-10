# -*- coding: utf-8 -*-
#!/usr/bin/env python
import os
import sys
import papermill as pm
from datetime import datetime as dt
import subprocess
import argparse
import warnings

warnings.filterwarnings("ignore")

def filename(nome):
    inicio = dt.now().strftime('%d_%m_%Y_%M%S')
    filename = 'RelatorioLattes-' + nome + '_' + inicio + '.ipynb'
    return filename

def run_notebook(nome):

    notebook_template = 'RelatorioPessoalLattesUFCG.ipynb'
    nome_arquivo = filename(nome)
    # run with papermill
    pm.execute_notebook(
        notebook_template,
        nome_arquivo,
        parameters=dict(nome=nome),
    )
    return nome_arquivo


def generate_html_report(filename):
    generate = subprocess.run(
        [
            "jupyter",
            "nbconvert",
            filename,
            "--no-input",
            "--no-prompt",
            "--to=html",
        ]
    )
    print("HTML Report was generated")
    return True

#-----------------------------------------------------------------------
def main():
    #parentDir = os.path.dirname(__file__)
    #PATH = '../../notebooks/'
    #newPath = os.path.join(parentDir, os.path.abspath(PATH))
    #caminho = os.path.abspath("../../src/pylattesLXML/")
    #os.chdir(caminho)
    #dirpath = os.getcwd()
    parser = argparse.ArgumentParser(description='Gera relatório individual de produção')
    parser.add_argument('--nome', required = True, help = 'Parâmetro nome para geração de relatório')
    args = parser.parse_args()
    nome = args.nome
    execTemplate = run_notebook(nome)
    try:
        generate_html_report(execTemplate)
    except:
        print('deu merda')
    return True


if __name__ == "__main__":
    main()
#-----------------------------------------------------------------------
