"""
Script auxiliar para adicionar novos usuários ao sistema NALA
Execução: python add_user.py
"""

import psycopg2
import bcrypt
import getpass

# Configuração do banco (mesmas credenciais do app principal)
DB_CONFIG = {
    'host': 'ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech',
    'database': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_fplFq8iAR4Ur',
    'port': '5432'
}

def hash_password(password):
    """Gera hash bcrypt da senha"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def listar_usuarios():
    """Lista todos os usuários cadastrados"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT username, role, ativo, created_at 
            FROM dim_usuarios 
            ORDER BY created_at DESC
        """)
        
        usuarios = cursor.fetchall()
        
        print("\n" + "="*70)
        print("USUÁRIOS CADASTRADOS")
        print("="*70)
        
        if usuarios:
            print(f"{'Username':<20} {'Role':<15} {'Ativo':<10} {'Criado em'}")
            print("-"*70)
            for user in usuarios:
                username, role, ativo, created = user
                status = "✅ Sim" if ativo else "❌ Não"
                data = created.strftime('%d/%m/%Y %H:%M')
                print(f"{username:<20} {role:<15} {status:<10} {data}")
        else:
            print("Nenhum usuário cadastrado ainda.")
        
        print("="*70 + "\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro ao listar usuários: {e}")

def adicionar_usuario():
    """Adiciona um novo usuário ao sistema"""
    print("\n" + "="*70)
    print("ADICIONAR NOVO USUÁRIO")
    print("="*70 + "\n")
    
    # Coletar informações
    username = input("Username (login): ").strip()
    
    if not username:
        print("❌ Username não pode ser vazio!")
        return
    
    # Senha
    while True:
        password = getpass.getpass("Senha: ")
        password_confirm = getpass.getpass("Confirme a senha: ")
        
        if password != password_confirm:
            print("❌ As senhas não coincidem! Tente novamente.\n")
        elif len(password) < 6:
            print("❌ A senha deve ter pelo menos 6 caracteres! Tente novamente.\n")
        else:
            break
    
    # Role
    print("\nPerfis disponíveis:")
    print("1. ADMIN - Acesso completo ao sistema")
    print("2. COMPRAS - Módulo de compras")
    print("3. GESTOR - Dashboards e calculadora")
    
    while True:
        opcao = input("\nEscolha o perfil (1-3): ").strip()
        
        if opcao == "1":
            role = "ADMIN"
            break
        elif opcao == "2":
            role = "COMPRAS"
            break
        elif opcao == "3":
            role = "GESTOR"
            break
        else:
            print("❌ Opção inválida! Digite 1, 2 ou 3.")
    
    # Confirmar
    print("\n" + "-"*70)
    print("RESUMO DO NOVO USUÁRIO:")
    print(f"  Username: {username}")
    print(f"  Perfil: {role}")
    print("-"*70)
    
    confirma = input("\nConfirmar criação? (s/n): ").strip().lower()
    
    if confirma != 's':
        print("❌ Operação cancelada.")
        return
    
    # Criar usuário
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Gerar hash da senha
        password_hash = hash_password(password)
        
        # Inserir no banco
        cursor.execute("""
            INSERT INTO dim_usuarios (username, password_hash, role)
            VALUES (%s, %s, %s)
        """, (username, password_hash, role))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n✅ Usuário criado com sucesso!")
        print(f"   Username: {username}")
        print(f"   Perfil: {role}")
        print("\nO usuário já pode fazer login no sistema.\n")
        
    except psycopg2.errors.UniqueViolation:
        print(f"\n❌ Erro: Usuário '{username}' já existe!")
        conn.rollback()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Erro ao criar usuário: {e}")
        if conn:
            conn.rollback()
            conn.close()

def alterar_senha():
    """Altera a senha de um usuário existente"""
    print("\n" + "="*70)
    print("ALTERAR SENHA DE USUÁRIO")
    print("="*70 + "\n")
    
    username = input("Username do usuário: ").strip()
    
    if not username:
        print("❌ Username não pode ser vazio!")
        return
    
    # Verificar se usuário existe
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("SELECT username FROM dim_usuarios WHERE username = %s", (username,))
        user = cursor.fetchone()
        
        if not user:
            print(f"❌ Usuário '{username}' não encontrado!")
            cursor.close()
            conn.close()
            return
        
        # Solicitar nova senha
        while True:
            password = getpass.getpass("Nova senha: ")
            password_confirm = getpass.getpass("Confirme a nova senha: ")
            
            if password != password_confirm:
                print("❌ As senhas não coincidem! Tente novamente.\n")
            elif len(password) < 6:
                print("❌ A senha deve ter pelo menos 6 caracteres! Tente novamente.\n")
            else:
                break
        
        # Gerar hash e atualizar
        password_hash = hash_password(password)
        
        cursor.execute("""
            UPDATE dim_usuarios 
            SET password_hash = %s, updated_at = CURRENT_TIMESTAMP
            WHERE username = %s
        """, (password_hash, username))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n✅ Senha alterada com sucesso para o usuário '{username}'!\n")
        
    except Exception as e:
        print(f"\n❌ Erro ao alterar senha: {e}")
        if conn:
            conn.rollback()
            conn.close()

def desativar_usuario():
    """Desativa um usuário (não pode mais fazer login)"""
    print("\n" + "="*70)
    print("DESATIVAR USUÁRIO")
    print("="*70 + "\n")
    
    username = input("Username do usuário: ").strip()
    
    if not username:
        print("❌ Username não pode ser vazio!")
        return
    
    if username == 'admin':
        print("❌ Não é possível desativar o usuário admin!")
        return
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE dim_usuarios 
            SET ativo = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE username = %s
            RETURNING username
        """, (username,))
        
        result = cursor.fetchone()
        
        if result:
            conn.commit()
            print(f"\n✅ Usuário '{username}' desativado com sucesso!\n")
        else:
            print(f"\n❌ Usuário '{username}' não encontrado!\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Erro ao desativar usuário: {e}")
        if conn:
            conn.rollback()
            conn.close()

def ativar_usuario():
    """Ativa um usuário desativado"""
    print("\n" + "="*70)
    print("ATIVAR USUÁRIO")
    print("="*70 + "\n")
    
    username = input("Username do usuário: ").strip()
    
    if not username:
        print("❌ Username não pode ser vazio!")
        return
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE dim_usuarios 
            SET ativo = TRUE, updated_at = CURRENT_TIMESTAMP
            WHERE username = %s
            RETURNING username
        """, (username,))
        
        result = cursor.fetchone()
        
        if result:
            conn.commit()
            print(f"\n✅ Usuário '{username}' ativado com sucesso!\n")
        else:
            print(f"\n❌ Usuário '{username}' não encontrado!\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Erro ao ativar usuário: {e}")
        if conn:
            conn.rollback()
            conn.close()

def menu_principal():
    """Menu principal do script"""
    while True:
        print("\n" + "="*70)
        print("NALA - GERENCIAMENTO DE USUÁRIOS")
        print("="*70)
        print("\n1. Listar usuários")
        print("2. Adicionar novo usuário")
        print("3. Alterar senha de usuário")
        print("4. Desativar usuário")
        print("5. Ativar usuário")
        print("0. Sair")
        print("\n" + "="*70)
        
        opcao = input("\nEscolha uma opção: ").strip()
        
        if opcao == "1":
            listar_usuarios()
        elif opcao == "2":
            adicionar_usuario()
        elif opcao == "3":
            alterar_senha()
        elif opcao == "4":
            desativar_usuario()
        elif opcao == "5":
            ativar_usuario()
        elif opcao == "0":
            print("\n👋 Até logo!\n")
            break
        else:
            print("\n❌ Opção inválida! Tente novamente.")

if __name__ == "__main__":
    print("\n🏪 NALA - Sistema de Gestão Marketplaces")
    print("📝 Script de Gerenciamento de Usuários\n")
    
    try:
        # Testar conexão
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("✅ Conexão com banco de dados OK\n")
        
        menu_principal()
        
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
        print("\nVerifique as credenciais no arquivo add_user.py\n")
