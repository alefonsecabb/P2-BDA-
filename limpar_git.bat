@echo off
:: Este script configura o .gitignore e limpa o cache do Git no Windows CMD

echo --- Iniciando limpeza do Git ---

:: 1. Verifica se existe repositório git aqui
if not exist .git (
    echo [ERRO] Nao encontrei a pasta .git. 
    echo Execute este script na raiz do seu projeto.
    pause
    exit /b
)

:: 2. Cria ou adiciona regras ao .gitignore
echo. >> .gitignore
echo # --- Regras adicionadas via Script --- >> .gitignore
echo .idea/ >> .gitignore
echo .vscode/ >> .gitignore
echo __pycache__/ >> .gitignore
echo *.py[cod] >> .gitignore
echo *$py.class >> .gitignore
echo .ipynb_checkpoints/ >> .gitignore
echo venv/ >> .gitignore
echo .env >> .gitignore
echo

echo [.gitignore atualizado com sucesso]

:: 3. Limpa o cache do Git (remove arquivos da area de staging)
echo.
echo [INFO] Removendo arquivos cacheados (isso pode demorar um pouco)...
git rm -r --cached .

:: 4. Adiciona tudo novamente (agora respeitando o .gitignore)
echo.
echo [INFO] Re-adicionando arquivos ao index...
git add .

:: 5. Commit automatico
echo.
echo [INFO] Realizando commit de limpeza...
git commit -m "chore: atualizando gitignore e removendo arquivos temporarios"

echo.
echo ---------------------------------------------------
echo PROCESSO CONCLUIDO!
echo Verifique se as pastas .idea ou __pycache__ sumiram do 'git status'.
echo ---------------------------------------------------
pause
