# financas

1. Crie uma conta no GitHub Você pode começar seguindo os passos mostrados no vídeo, preenchendo o formulário de cadastro, verificando sua conta e explorando a página inicial do GitHub. 

2. Criar um novo repositório no GitHub e fazer o upload de um projeto local para esse repositório. Você pode seguir passos mencionados na aula, como acessar as configurações do GitHub, criar um novo repositório com um nome único e escolher se ele será público ou privado. Em seguida, você pode adicionar uma descrição, um README, um .gitignore e uma licença ao repositório. 

3. Instalação do Git: Caso você ainda não tenha realizado a instalação, siga os passos na atividade [Faça como eu fiz: instalação do Git](https://cursos.alura.com.br/course/git-github-compartilhando-colaborando-projetos/task/144999) 

4. Para criar um repositório local você, digite o seguinte comando no terminal: 
    git init

5. Para adicionar os arquivos no repositório local, digite no terminal o comando: 
    git add .

6. Faça um commit com as modificações, digite no terminal o comando: 
    git commit -m "mensagem de commit"

7. Para configurar a identidade do autor do commit, digite no terminal o comando: 
    git config --global user.email "seuemailaqui@example.com" 
    git config --global user.name "seu nome aqui"

8. Para criar a branch Main, digite no terminal o comando: 
    git branch -M main
O comando git branch -m é usado para criar uma nova ramificação no repositório Git atual. Neste caso, criamos a branch padrão main, que representa a versão principal do código. 

9. Para realizar a conexão do seu repositório local com o remoto via SSH, digite no terminal: 
git remote add origin git@github.com:seunomedeusuario/seu-repositorio.git
Caso seja necessário, realize a configuração do protocolo SSH através da geração de chave, você pode acompanhar os passos em vídeo na atividade [Sincronizando repositórios](https://cursos.alura.com.br/course/git-github-compartilhando-colaborando-projetos/task/139310) 

10. Para subir as alterações no repositório local para o remoto, digite o seguinte comando no terminal: 
    git push -u origin main 

11. Digite no terminal o comando "git status" e observe a saída. O comando "git status" é uma ferramenta essencial para gerenciar alterações no controle de versão Git. 
Ele fornece uma visão geral do estado atual do repositório, indicando quais arquivos foram modificados, adicionados ou excluídos desde o último commit. 
Essa informação é crucial para compreender o progresso do desenvolvimento e tomar decisões de gerenciamento de alterações. 
A saída do comando "git status" geralmente contém três seções principais: 
**Modificados:** Lista os arquivos que foram modificados desde o último commit, mas ainda não foram adicionados à área de preparação (Stage). 
**Adicionados:** Indica os arquivos que foram adicionados à área de preparação, mas ainda não foram confirmados no histórico de commits. 
**Modificados, adicionados ou excluídos:** Exibe os arquivos que não foram rastreados pelo Git, ou seja, que não foram adicionados ao índice de modificações (Staging Area). 
Além disso, o comando "git status" pode fornecer informações adicionais sobre as ramificações atuais, como a ramificação atual e as ramificações que estão à frente ou atrás da atual. 
O comando "git status" é uma ferramenta indispensável para qualquer desenvolvedor que utiliza Git. Ele permite monitorar as alterações no repositório, identificar o estado dos arquivos e tomar decisões de gerenciamento de commits de forma eficiente.

===================================================================================================
GITHUB

## inicia o git no projeto
git init

## adiciona todos os arquivos ao git. para adicionar somente um arquivo especificar o nome do arquivo no lugar do "."
git add .


git commit -m "informar alguma coisa sobre o projeto"


## configuração para informar usuario do git no computador. deve solicitar somente na primeira vez que associa o usuario no git do computador
git config --global user.mail "email de cadastro do git"
git config --global user.name "meu nome"


git branch -m main     

## sincronizar o repositorio local no github
git remote add origin git@github.com:rodrigomorau/financas.git

## verificar o apelido do repositorio
git remote -v

## enviar os arquivos para o github
git push -u origin main

## para clonar um repositório
git clone url
