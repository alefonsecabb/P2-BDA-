#!/bin/bash

# Cria o arquivo .gitignore
cat <<EOT >> .gitignore
# --- Regras Automaticas ---
.idea/
.vscode/
__pycache__/
*.py[cod]
*$py.class
.ipynb_checkpoints/
venv/
.env
EOT

# Executa os comandos do git
git rm -r --cached .
git add .
git commit -m "chore: atualizando gitignore e removendo arquivos temporarios"

echo "Processo concluído!"