
# <p align="center"> <img src="./assets/Logo-UFCG.png" width="80" ></p>

# pyLattes-LXML
## Luciano Barosi

Este módulo foi desenvolvido para fazer o parsing de currículos Lattes no formato XML. Suas principais funcionalidades são:
1. Produção de lista de nomes - Currículos para garantir que apenas um currículo de cada pesquisador seja carregado.
2. Rotina para geração de relatórios individuais com sumérios e produção total.
3. Rotina para analisar em bloco um conjunto de currículos e efetuar a pontuação segundo as regras atuais da UFCG. A alteração das regras e de todos os parâmetros pode ser feita sem a necessidade de alterar o código fonte.
4. Conjunto de script de linha de comando.
5. Os curriculos devem ser colocados na pasta pylattes-lxml/data/external
6. Todas as funções já estão funcionando.
7. Documentação ainda em construção.
8. CLI ainda a fazer.

Contato: lbarosi@df.ufcg.edu.br

## Uso individual.

- Coloque o xml de seu currículo lattes em pylattes-lxml/data/external
- No terminal, navegue até a pasta 'notebooks/'
- Execute:
```bash
python relatorio.py --nome='seu nome aqui'
```

Isto vai gerar um arquivo HTML na pasta **notebooks** com a análise de sua produção.
