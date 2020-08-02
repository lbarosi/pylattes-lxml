# Mais detalhes em breve


# Instalação do Python

Estas instruções são para um sistema Linux de 64 bits. Mas pode ser realizada em Windows ou MacOs, fazendo as alterações adequadas ao seu sistema operacional.

1. Baixe o `miniconda`
2. Crie um ambiente
3. Inicie seu ambiente

Os passos acima são executados com os seguintes comandos:

```` bash
cd MEU_CAMINHO/
git clone https://github.com/lbarosi/pyLattesLXML.git
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod u+x ./Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
cd pyLattesLXML
conda update conda
conda env create -f environment.yml
conda activate lattes
pip install ipykernel
python -m ipykernel install --name=lattes
````
