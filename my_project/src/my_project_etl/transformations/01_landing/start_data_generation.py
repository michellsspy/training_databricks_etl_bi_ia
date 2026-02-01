# Célula 1: Instalações e Imports (Sem alterações)
# !pip install Faker names

import random
import names
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Inicializa o Faker
fake = Faker('pt_BR')

# Inicializa a Sessão Spark (no Databricks, ela já existe como 'spark')
spark = SparkSession.builder.appName("GeracaoDadosHotelaria").getOrCreate()

print("Faker e Spark inicializados.")

# Célula 2: Funções de Geração - HOTEIS (Sem alterações)
def gerar_hoteis(num_hoteis=50):
    COMODIDADES = ['Wi-Fi Grátis', 'Piscina', 'Academia', 'Spa', 'Estacionamento', 'Restaurante', 'Bar', 'Serviço de Quarto', 'Centro de Convenções', 'Pet Friendly']
    PREFIXOS = ['Grand', 'Royal', 'Plaza', 'Imperial', 'Golden', 'Paradise', 'Ocean']
    SUFIXOS = ['Hotel', 'Resort', 'Palace', 'Inn', 'Suites']
    CATEGORIAS_HOTEL = ['Econômico', 'Médio', 'Superior', 'Luxo', 'Boutique', 'Business']
    TIPOS_HOTEL = ['Urbano', 'Praia', 'Montanha', 'Campo', 'Histórico', 'Familiar']
    
    hoteis_data = []
    for i in range(num_hoteis):
        nome_hotel = f"{random.choice(PREFIXOS)} {fake.city_suffix()} {random.choice(SUFIXOS)}"
        num_comodidades = random.randint(3, 7)
        comodidades_selecionadas = random.sample(COMODIDADES, num_comodidades)
        
        # Novas colunas calculadas/enriquecidas
        ano_fundacao = random.randint(1980, 2020)
        capacidade_total = random.randint(100, 500)
        possui_acessibilidade = random.choice([True, False])
        certificacoes = random.sample(['ISO 9001', 'Sustentável', 'Green Key', 'TripAdvisor Excellence'], random.randint(0, 2))
        
        hotel = (
            101 + i,
            nome_hotel,
            fake.street_address(),
            fake.city(),
            fake.state_abbr(),
            random.randint(3, 5),
            random.randint(50, 250),
            ', '.join(comodidades_selecionadas),
            fake.phone_number(),
            f"contato@{nome_hotel.lower().split(' ')[0]}.com",
            fake.past_date(start_date="-20y"),
            "14:00",
            "12:00",
            random.choice(CATEGORIAS_HOTEL),  # Nova coluna
            random.choice(TIPOS_HOTEL),       # Nova coluna
            ano_fundacao,                     # Nova coluna
            capacidade_total,                 # Nova coluna
            possui_acessibilidade,            # Nova coluna
            ', '.join(certificacoes),         # Nova coluna
            round(random.uniform(-23.5, -23.7), 6),  # Nova coluna: latitude
            round(random.uniform(-46.6, -46.8), 6),  # Nova coluna: longitude
            fake.text(max_nb_chars=200),      # Nova coluna: descrição
            random.randint(50, 200)           # Nova coluna: número de funcionários
        )
        hoteis_data.append(hotel)
        
    schema = StructType([
        StructField("hotel_id", IntegerType(), False),
        StructField("nome_hotel", StringType(), True),
        StructField("endereco", StringType(), True),
        StructField("cidade", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("estrelas", IntegerType(), True),
        StructField("numero_quartos", IntegerType(), True),
        StructField("comodidades", StringType(), True),
        StructField("telefone", StringType(), True),
        StructField("email_contato", StringType(), True),
        StructField("data_abertura", DateType(), True),
        StructField("horario_checkin", StringType(), True),
        StructField("horario_checkout", StringType(), True),
        StructField("categoria_hotel", StringType(), True),      # Nova coluna
        StructField("tipo_hotel", StringType(), True),           # Nova coluna
        StructField("ano_fundacao", IntegerType(), True),        # Nova coluna
        StructField("capacidade_total", IntegerType(), True),    # Nova coluna
        StructField("possui_acessibilidade", BooleanType(), True), # Nova coluna
        StructField("certificacoes", StringType(), True),        # Nova coluna
        StructField("latitude", DoubleType(), True),             # Nova coluna
        StructField("longitude", DoubleType(), True),            # Nova coluna
        StructField("descricao_hotel", StringType(), True),      # Nova coluna
        StructField("numero_funcionarios", IntegerType(), True)  # Nova coluna
    ])
    
    df = spark.createDataFrame(hoteis_data, schema)
    return df

# Célula 3: Funções de Geração - QUARTOS (Sem alterações)
def gerar_quartos(df_hoteis):
    TIPOS_QUARTO = {
        'Standard': {'capacidade': [1, 2], 'percentual': 0.50, 'preco_base': 250.0},
        'Superior': {'capacidade': [2, 3], 'percentual': 0.25, 'preco_base': 400.0},
        'Suíte': {'capacidade': [2, 4], 'percentual': 0.15, 'preco_base': 650.0},
        'Suíte Presidencial': {'capacidade': [2, 5], 'percentual': 0.10, 'preco_base': 1200.0}
    }
    VISTAS = ['Cidade', 'Piscina', 'Mar', 'Jardim', 'Interna']
    COMODIDADES_QUARTO = ['Frigobar', 'Varanda', 'Banheira', 'Cofre', 'Smart TV', 'Ar Condicionado Central', 'Cafeteira', 'Secador de Cabelo']
    TAMANHOS_QUARTO = ['Pequeno (20m²)', 'Médio (30m²)', 'Grande (45m²)', 'Amplo (60m²+)']
    
    quartos_data = []
    quarto_id_global = 1001
    hoteis_info = df_hoteis.collect()
    
    for hotel in hoteis_info:
        total_quartos_hotel = hotel['numero_quartos']
        
        tipos_dist = []
        tipos_count = {tipo: int(total_quartos_hotel * config['percentual']) for tipo, config in TIPOS_QUARTO.items()}
        total_distribuido = sum(tipos_count.values())
        tipos_count['Standard'] += total_quartos_hotel - total_distribuido 
        
        for tipo, count in tipos_count.items():
            tipos_dist.extend([tipo] * count)
        
        random.shuffle(tipos_dist)
            
        for i in range(total_quartos_hotel):
            tipo_quarto_atual = tipos_dist[i]
            config = TIPOS_QUARTO[tipo_quarto_atual]
            preco_final = config['preco_base'] * (1 + (hotel['estrelas'] - 3) * 0.15)
            
            andar = (i // 20) + 1
            numero_quarto_str = f"{andar:02d}{(i % 20) + 1:02d}"
            
            num_comodidades_qto = random.randint(2, len(COMODIDADES_QUARTO))
            comodidades_qto_selecionadas = random.sample(COMODIDADES_QUARTO, num_comodidades_qto)
            
            # Novas colunas para quartos
            possui_ar_condicionado = 'Ar Condicionado Central' in comodidades_qto_selecionadas
            tamanho_quarto = random.choice(TAMANHOS_QUARTO)
            status_manutencao = random.choices(['Disponível', 'Em Manutenção', 'Em Limpeza'], weights=[0.85, 0.05, 0.10], k=1)[0]
            ultima_manutencao = fake.past_date(start_date="-180d") if random.random() < 0.7 else None
            eh_smoke_free = random.choice([True, False])
            possui_kit_boas_vindas = random.choice([True, False])
            
            quarto = (
                quarto_id_global,
                hotel['hotel_id'],
                numero_quarto_str,
                tipo_quarto_atual,
                random.randint(config['capacidade'][0], config['capacidade'][1]),
                round(preco_final, 2),
                andar,
                random.choice(VISTAS),
                ', '.join(comodidades_qto_selecionadas),
                possui_ar_condicionado,           # Nova coluna
                tamanho_quarto,                   # Nova coluna
                status_manutencao,                # Nova coluna
                ultima_manutencao,                # Nova coluna
                eh_smoke_free,                    # Nova coluna
                possui_kit_boas_vindas,           # Nova coluna
                random.randint(1, 4)              # Nova coluna: número de camas
            )
            quartos_data.append(quarto)
            quarto_id_global += 1
            
    schema = StructType([
        StructField("quarto_id", IntegerType(), False),
        StructField("hotel_id", IntegerType(), False),
        StructField("numero_quarto", StringType(), True),
        StructField("tipo_quarto", StringType(), True),
        StructField("capacidade_maxima", IntegerType(), True),
        StructField("preco_diaria_base", DoubleType(), True),
        StructField("andar", IntegerType(), True),
        StructField("vista", StringType(), True),
        StructField("comodidades_quarto", StringType(), True),
        StructField("possui_ar_condicionado", BooleanType(), True),  # Nova coluna
        StructField("tamanho_quarto", StringType(), True),           # Nova coluna
        StructField("status_manutencao", StringType(), True),        # Nova coluna
        StructField("ultima_manutencao", DateType(), True),          # Nova coluna
        StructField("eh_smoke_free", BooleanType(), True),           # Nova coluna
        StructField("possui_kit_boas_vindas", BooleanType(), True),  # Nova coluna
        StructField("numero_camas", IntegerType(), True)             # Nova coluna
    ])
    
    return spark.createDataFrame(quartos_data, schema)

# Célula 4: Funções de Geração - HÓSPEDES (Sem alterações)
def gerar_hospedes(num_hospedes=8000):
    
    PROGRAMAS_FIDELIDADE = ['Bronze', 'Prata', 'Gold', 'Platinum']
    PROFISSOES = ['Engenheiro', 'Médico', 'Professor', 'Empresário', 'Estudante', 'Aposentado', 'Artista', 'Tecnologia', 'Comércio', 'Servidor Público', None]
    TIPOS_DOCUMENTO = ['CPF', 'Passaporte', 'RG', 'CNH']
    
    hospedes_data = []
    for i in range(num_hospedes):
        nome = fake.name()
        data_nasc = fake.date_of_birth(minimum_age=18, maximum_age=85)
        data_cadastro_start = data_nasc + timedelta(days=18*365)
        data_cadastro = fake.date_time_between(start_date=data_cadastro_start, end_date="now").date()
        
        # Novas colunas para hóspedes
        profissao = random.choice(PROFISSOES)
        tipo_documento = random.choice(TIPOS_DOCUMENTO)
        numero_documento = fake.cpf() if tipo_documento == 'CPF' else fake.license_plate().replace('-', '')
        empresa = fake.company() if random.random() < 0.4 else None
        eh_viajante_frequente = random.choice([True, False])
        preferencias = random.sample(['Quieto', 'Café da manhã', 'Jornal', 'Toalhas extras', 'Travesseiro especial'], random.randint(0, 3))
        restricoes_alimentares = random.choice([None, 'Vegetariano', 'Vegano', 'Sem glúten', 'Sem lactose'])
        data_ultima_hospedagem = fake.past_date(start_date="-365d") if random.random() < 0.6 else None
        
        hospede = (
            2001 + i,
            nome,
            fake.cpf(),
            data_nasc,
            f"{nome.split(' ')[0].lower()}_{i}@email.com",
            fake.phone_number(),
            fake.state_abbr(),
            'Brasil',
            data_cadastro,
            random.choice(PROGRAMAS_FIDELIDADE),
            profissao,                          # Nova coluna
            tipo_documento,                     # Nova coluna
            numero_documento,                   # Nova coluna
            empresa,                            # Nova coluna
            eh_viajante_frequente,              # Nova coluna
            ', '.join(preferencias) if preferencias else None,  # Nova coluna
            restricoes_alimentares,             # Nova coluna
            data_ultima_hospedagem,             # Nova coluna
            random.randint(0, 50)               # Nova coluna: total de hospedagens
        )
        hospedes_data.append(hospede)
        
    schema = StructType([
        StructField("hospede_id", IntegerType(), False),
        StructField("nome_completo", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("data_nascimento", DateType(), True),
        StructField("email", StringType(), True),
        StructField("telefone", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("nacionalidade", StringType(), True),
        StructField("data_cadastro", DateType(), True),
        StructField("programa_fidelidade", StringType(), True),
        StructField("profissao", StringType(), True),           # Nova coluna
        StructField("tipo_documento", StringType(), True),      # Nova coluna
        StructField("numero_documento", StringType(), True),    # Nova coluna
        StructField("empresa", StringType(), True),             # Nova coluna
        StructField("eh_viajante_frequente", BooleanType(), True), # Nova coluna
        StructField("preferencias_hospede", StringType(), True), # Nova coluna
        StructField("restricoes_alimentares", StringType(), True), # Nova coluna
        StructField("data_ultima_hospedagem", DateType(), True), # Nova coluna
        StructField("total_hospedagens", IntegerType(), True)   # Nova coluna
    ])
    
    return spark.createDataFrame(hospedes_data, schema)

# Célula 5: Funções de Geração - RESERVAS, CONSUMOS, FATURAS e RESERVAS_OTA (MODIFICADA)
def gerar_dependentes(df_hoteis, df_quartos, df_hospedes, num_reservas=15000):
    
    CANAIS = ['Website Hotel', 'Booking.com', 'Expedia', 'Telefone', 'Balcão', 'Agência de Viagem']
    # NOVO: Lista de canais que são OTAs
    OTAS = ['Booking.com', 'Expedia'] 
    
    SERVICOS = [
        ('Restaurante - Jantar', 120.0),
        ('Restaurante - Bebida', 15.0),
        ('Bar - Coquetel', 35.0),
        ('Serviço de Quarto - Lanche', 50.0),
        ('Frigobar - Refrigerante', 8.0),
        ('Frigobar - Água', 6.0),
        ('Massagem Relaxante', 250.0),
        ('Tratamento Facial', 180.0),
        ('Lavanderia - Peça', 20.0),
        ('Estacionamento - Diária', 40.0),
        ('Translado Aeroporto', 80.0),
        ('Passeio Guiado', 150.0),
        ('Aluguel de Carro', 200.0)
    ]
    
    FIDELIDADE_DESCONTOS = {'Bronze': 0.0, 'Prata': 0.05, 'Gold': 0.10, 'Platinum': 0.15}
    FORMAS_PAGAMENTO = ['Cartão de Crédito', 'Pix', 'Dinheiro', 'Cartão de Débito', 'Transferência Bancária']
    SOLICITACOES = ['', 'Berço', 'Andar alto', 'Check-in antecipado', 'Quarto para não fumantes', 'Cama casal', 'Vista mar', 'Quarto térreo']
    MOTIVOS_VIAGEM = ['Lazer', 'Negócios', 'Família', 'Romance', 'Evento', 'Saúde']
    
    HOJE = datetime.now().date()
    TAXA_IMPOSTO = 0.05
    
    quartos_info = df_quartos.select("quarto_id", "hotel_id", "preco_diaria_base", "capacidade_maxima").collect()
    hospedes_info = df_hospedes.select("hospede_id", "programa_fidelidade").collect()

    reservas_data = []
    consumos_data = []
    faturas_data = []
    reservas_ota_data = [] # NOVO: Lista para a nova tabela
    
    consumo_id_global = 50001
    fatura_id_global = 9001
    ota_reserva_id_global = 7001 # NOVO: ID Global para a nova tabela

    for i in range(num_reservas):
        reserva_id = 10001 + i
        quarto_selecionado = random.choice(quartos_info)
        hospede_selecionado = random.choice(hospedes_info)
        hospede_id = hospede_selecionado['hospede_id']
        
        # Datas da reserva
        dias_antecedencia = random.randint(1, 90)
        data_reserva = datetime.now() - timedelta(days=dias_antecedencia + random.randint(5, 365*2))
        data_checkin = data_reserva + timedelta(days=dias_antecedencia)
        num_noites = random.randint(1, 10)
        data_checkout = data_checkin + timedelta(days=num_noites)

        # Status da Reserva
        if data_checkout.date() < HOJE:
            status_reserva = 'Concluída'
        elif data_checkin.date() < HOJE and data_checkout.date() > HOJE:
            status_reserva = 'Hospedado' 
        elif data_checkin.date() > HOJE:
            status_reserva = random.choices(['Confirmada', 'Cancelada'], weights=[0.9, 0.1], k=1)[0]
        else:
             status_reserva = 'Concluída'
        
        data_cancelamento = None
        motivo_cancelamento = None
        if status_reserva == 'Cancelada':
            dias_para_cancelar = random.randint(1, dias_antecedencia - 1 if dias_antecedencia > 1 else 1)
            data_cancelamento = data_reserva + timedelta(days=dias_para_cancelar)
            motivo_cancelamento = random.choice(['Plano alterado', 'Problemas de saúde', 'Problemas financeiros', 'Encontrou opção melhor', 'Motivos pessoais'])

        # Ocupação
        num_adultos = random.randint(1, quarto_selecionado['capacidade_maxima'])
        capacidade_restante = quarto_selecionado['capacidade_maxima'] - num_adultos
        num_criancas = random.randint(0, capacidade_restante)

        # Dados da Reserva
        valor_base_estadia = num_noites * quarto_selecionado['preco_diaria_base']
        taxa_limpeza = round(valor_base_estadia * 0.02, 2)
        taxa_turismo = round(valor_base_estadia * 0.03, 2)
        canal_reserva = random.choice(CANAIS) # Define o canal
        
        reservas_data.append((
            reserva_id, hospede_id, quarto_selecionado['quarto_id'], quarto_selecionado['hotel_id'],
            data_reserva.date(), data_checkin.date(), data_checkout.date(), num_noites,
            num_adultos,
            num_criancas,
            canal_reserva, # Usa a variável
            status_reserva,
            data_cancelamento.date() if data_cancelamento else None,
            random.choice(SOLICITACOES),
            valor_base_estadia,
            random.choice(MOTIVOS_VIAGEM),
            motivo_cancelamento,
            taxa_limpeza,
            taxa_turismo,
            round(random.uniform(4.0, 5.0), 1) if status_reserva == 'Concluída' and random.random() < 0.7 else None,
            fake.text(max_nb_chars=100) if status_reserva == 'Concluída' and random.random() < 0.3 else None
        ))
        
        # --- NOVO: GERAÇÃO DE DADOS RESERVAS_OTA ---
        # Se a reserva veio de um canal OTA, cria um registro na tabela OTA
        if canal_reserva in OTAS and status_reserva != 'Cancelada':
            ota_reserva_id = ota_reserva_id_global
            ota_codigo_confirmacao = f"{canal_reserva[0:3].upper()}-{fake.pystr(8, 8, '0123456789ABCDEF')}"
            taxa_comissao = round(random.uniform(0.12, 0.25), 2) # Comissão entre 12% e 25%
            
            # O valor que o hóspede pagou na OTA (pode ser um pouco diferente)
            total_pago_ota = valor_base_estadia + taxa_limpeza + taxa_turismo
            
            # O valor líquido que o hotel recebe (Valor pago - Comissão)
            valor_liquido_recebido = round(total_pago_ota * (1 - taxa_comissao), 2)
            
            # Nome do hóspede como veio da OTA (pode ser ligeiramente diferente)
            ota_nome_convidado = fake.name()
            
            ota_solicitacoes_especificas = fake.text(max_nb_chars=50) if random.random() < 0.3 else None
            
            reservas_ota_data.append((
                ota_reserva_id,
                reserva_id, # Chave estrangeira para a tabela de reservas
                ota_codigo_confirmacao,
                ota_nome_convidado,
                round(total_pago_ota, 2),
                taxa_comissao,
                valor_liquido_recebido,
                ota_solicitacoes_especificas
            ))
            ota_reserva_id_global += 1

        # --- Consumos e Faturas (Lógica existente) ---
        subtotal_consumos = 0.0
        if status_reserva in ['Concluída', 'Hospedado']:
            
            num_consumos_reserva = random.randint(0, 5)
            for _ in range(num_consumos_reserva):
                servico_selecionado = random.choice(SERVICOS)
                servico_nome = servico_selecionado[0]
                servico_preco_base = servico_selecionado[1]
                
                quantidade = random.randint(1, 3)
                valor_consumo_total = round(servico_preco_base * quantidade, 2)

                dias_na_estadia = random.randint(0, num_noites -1 if num_noites > 0 else 0)
                data_consumo = data_checkin + timedelta(days=dias_na_estadia)
                hora_consumo = f"{random.randint(6, 23):02d}:{random.randint(0, 59):02d}"
                
                consumos_data.append((
                    consumo_id_global, reserva_id, hospede_id, quarto_selecionado['hotel_id'],
                    servico_nome,
                    data_consumo.date(),
                    quantidade,
                    valor_consumo_total,
                    hora_consumo,
                    random.choice(['Quarto', 'Restaurante', 'Bar', 'Spa', 'Balcão']),
                    fake.name() if random.random() < 0.5 else None
                ))
                subtotal_consumos += valor_consumo_total
                consumo_id_global += 1

            # Dados da Fatura
            if status_reserva == 'Concluída':
                data_emissao = data_checkout + timedelta(days=1)
                
                fator_desconto = FIDELIDADE_DESCONTOS.get(hospede_selecionado['programa_fidelidade'], 0.0)
                desconto_valor = round(valor_base_estadia * fator_desconto, 2)
                
                subtotal_pos_desconto = (valor_base_estadia + subtotal_consumos + taxa_limpeza + taxa_turismo) - desconto_valor
                impostos_valor = round(subtotal_pos_desconto * TAXA_IMPOSTO, 2)
                valor_total_fatura = subtotal_pos_desconto + impostos_valor
                status_pagamento = random.choices(['Pago', 'Pendente', 'Atrasado'], weights=[0.85, 0.10, 0.05], k=1)[0]
                data_pagamento = data_emissao + timedelta(days=random.randint(0, 5)) if status_pagamento == 'Pago' else None
                
                faturas_data.append((
                    fatura_id_global, reserva_id, hospede_id, 
                    data_emissao.date(),
                    data_emissao.date() + timedelta(days=15),
                    status_pagamento,
                    random.choice(FORMAS_PAGAMENTO),
                    valor_base_estadia,
                    subtotal_consumos,
                    desconto_valor,
                    impostos_valor,
                    valor_total_fatura,
                    data_pagamento,
                    taxa_limpeza,
                    taxa_turismo,
                    round(random.uniform(0.0, 0.10), 4),
                    fake.iban()
                ))
                fatura_id_global += 1

    # --- Schemas --- (Schemas de reservas, consumos e faturas mantidos)
    schema_reservas = StructType([
        StructField("reserva_id", IntegerType(), False), StructField("hospede_id", IntegerType(), True),
        StructField("quarto_id", IntegerType(), True), StructField("hotel_id", IntegerType(), True),
        StructField("data_reserva", DateType(), True), StructField("data_checkin", DateType(), True),
        StructField("data_checkout", DateType(), True), StructField("numero_noites", IntegerType(), True),
        StructField("numero_adultos", IntegerType(), True),
        StructField("numero_criancas", IntegerType(), True),
        StructField("canal_reserva", StringType(), True),
        StructField("status_reserva", StringType(), True),
        StructField("data_cancelamento", DateType(), True),
        StructField("solicitacoes_especiais", StringType(), True),
        StructField("valor_total_estadia", DoubleType(), True),
        StructField("motivo_viagem", StringType(), True),
        StructField("motivo_cancelamento", StringType(), True),
        StructField("taxa_limpeza", DoubleType(), True),
        StructField("taxa_turismo", DoubleType(), True),
        StructField("avaliacao_hospede", DoubleType(), True),
        StructField("comentarios_hospede", StringType(), True)
    ])
    
    schema_consumos = StructType([
        StructField("consumo_id", IntegerType(), False), StructField("reserva_id", IntegerType(), True),
        StructField("hospede_id", IntegerType(), True), StructField("hotel_id", IntegerType(), True),
        StructField("nome_servico", StringType(), True),
        StructField("data_consumo", DateType(), True), 
        StructField("quantidade", IntegerType(), True),
        StructField("valor_total_consumo", DoubleType(), True),
        StructField("hora_consumo", StringType(), True),
        StructField("local_consumo", StringType(), True),
        StructField("funcionario_responsavel", StringType(), True)
    ])
    
    schema_faturas = StructType([
        StructField("fatura_id", IntegerType(), False), StructField("reserva_id", IntegerType(), True),
        StructField("hospede_id", IntegerType(), True), 
        StructField("data_emissao", DateType(), True),
        StructField("data_vencimento", DateType(), True),
        StructField("status_pagamento", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("subtotal_estadia", DoubleType(), True),
        StructField("subtotal_consumos", DoubleType(), True),
        StructField("descontos", DoubleType(), True),
        StructField("impostos", DoubleType(), True),
        StructField("valor_total", DoubleType(), True),
        StructField("data_pagamento", DateType(), True),
        StructField("taxa_limpeza", DoubleType(), True),
        StructField("taxa_turismo", DoubleType(), True),
        StructField("taxa_servico", DoubleType(), True),
        StructField("numero_transacao", StringType(), True)
    ])
    
    # NOVO: Schema para a tabela reservas_ota
    schema_reservas_ota = StructType([
        StructField("ota_reserva_id", IntegerType(), False), # Chave primária da tabela
        StructField("reserva_id", IntegerType(), False),     # Chave estrangeira (FK de source_reservas)
        StructField("ota_codigo_confirmacao", StringType(), True), # Cód. da OTA (ex: BKG-12345)
        StructField("ota_nome_convidado", StringType(), True),   # Nome do hóspede na OTA
        StructField("total_pago_ota", DoubleType(), True),# Valor total pago na OTA
        StructField("taxa_comissao", DoubleType(), True),  # Taxa de comissão (ex: 0.18 para 18%)
        StructField("valor_liquido_recebido", DoubleType(), True), # Valor líquido para o hotel
        StructField("ota_solicitacoes_especificas", StringType(), True) # Pedidos feitos na OTA
    ])

    # Criar DataFrames
    df_reservas = spark.createDataFrame(reservas_data, schema_reservas)
    df_consumos = spark.createDataFrame(consumos_data, schema_consumos)
    df_faturas = spark.createDataFrame(faturas_data, schema_faturas)
    df_reservas_ota = spark.createDataFrame(reservas_ota_data, schema_reservas_ota) # NOVO

    # MODIFICADO: Retorna o novo DataFrame
    return df_reservas, df_consumos, df_faturas, df_reservas_ota

# Célula 6: Execução Principal (MODIFICADA)
def run_generation():
    # Parâmetros de Governança
    CATALOG = "prod"
    SCHEMA = "transient"
    SISTEMA = "hotel_management" #Não está em uso
    DATA_REF = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Garantir existência do ambiente
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    
    print("="*80)
    print(f"INICIANDO GERAÇÃO TRANSIENT | MODO: FULL (MOCK) | DATA: {DATA_REF}")
    print("="*80)

    try:
        # 1. Hoteis
        print(f"[*] Gerando Entidade: Hoteis...")
        df_hoteis = gerar_hoteis(num_hoteis=50)
        table_hoteis = f"{CATALOG}.{SCHEMA}.source_hoteis"
        df_hoteis.write.mode("overwrite").saveAsTable(table_hoteis)
        print(f"[i] Volumetria: {df_hoteis.count()} registros | Colunas: {len(df_hoteis.columns)}")
        print(f"└─ Status: ✅ SUCESSO | Tabela: {table_hoteis}")

        # 2. Quartos
        print(f"[*] Gerando Entidade: Quartos...")
        df_quartos = gerar_quartos(df_hoteis)
        table_quartos = f"{CATALOG}.{SCHEMA}.source_quartos"
        df_quartos.write.mode("overwrite").saveAsTable(table_quartos)
        print(f"[i] Volumetria: {df_quartos.count()} registros | Colunas: {len(df_quartos.columns)}")
        print(f"└─ Status: ✅ SUCESSO | Tabela: {table_quartos}")

        # 3. Hóspedes
        print(f"[*] Gerando Entidade: Hospedes...")
        df_hospedes = gerar_hospedes(num_hospedes=8000)
        table_hospedes = f"{CATALOG}.{SCHEMA}.source_hospedes"
        df_hospedes.write.mode("overwrite").saveAsTable(table_hospedes)
        print(f"[i] Volumetria: {df_hospedes.count()} registros | Colunas: {len(df_hospedes.columns)}")
        print(f"└─ Status: ✅ SUCESSO | Tabela: {table_hospedes}")

        # 4. Processamento de Dependentes
        print(f"[*] Gerando Entidades Dependentes (Reservas, Consumos, Faturas, OTA)...")
        df_res, df_cons, df_fat, df_ota = gerar_dependentes(df_hoteis, df_quartos, df_hospedes, num_reservas=15000)
        
        entidades_dependentes = {
            "source_reservas": df_res,
            "source_consumos": df_cons,
            "source_faturas": df_fat,
            "source_reservas_ota": df_ota
        }

        for i, (nome_tab, df) in enumerate(entidades_dependentes.items(), 1):
            full_name = f"{CATALOG}.{SCHEMA}.{nome_tab}"
            print(f"[{i}/{len(entidades_dependentes)}] PROCESSANDO: {nome_tab.upper()}")
            df.write.mode("overwrite").saveAsTable(full_name)
            print(f" └─ Status: ✅ SUCESSO | Linhas Escritas: {df.count()} | Tabela: {full_name}")

    except Exception as e:
        print(f"❌ FALHA: Erro crítico na geração da Transient. Detalhes: {str(e)}")
        raise e

    print("="*80)
    print("PROCESSO FINALIZADO | CAMADA TRANSIENT PRONTA PARA CONSUMO BRONZE")
    print("="*80)

# Executa a função principal
run_generation()