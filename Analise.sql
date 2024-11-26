--Levantando a volumetria de atendimentos finalizados para definição de produtividade
WITH atendimentosFinalizados AS (
  SELECT
    operação AS parceira,
    squad AS squad,
    agent,
    local_start_date AS date,
    activity_type,
    net_time_spent,
    COUNT(*) AS total_finalizado
  FROM `dataset.atendimentos_finalizados`
  WHERE 
    local_start_date >= '2024-01-01'
    AND (operação = 'operação1' OR operação = 'operação2')
    AND status = 'finalizado'
    AND squad = 'setor1'
    AND activity_type IN ("chat", "ligação", "e-mail")
  GROUP BY
    operação,
    squad,
    agent,
    local_start_date,
    activity_type,
    net_time_spent
),
--Explorando a base de disponibilidade e levantando o tempo em que cada operador fica disponível hoje na ferramenta de atendimento
  
AgentMetrics AS (
  SELECT
    date,
    operação AS parceira,
    squad,
    agent,
    SUM(duration)/3600 AS disponibilidade_hora,
    SUM(duration) AS disponibilidade_seg
  FROM `dataset.disponibilidade`
  WHERE 
    date >= '2024-01-01'
    AND activity_type IN ("chat", "ligação", "email")
    AND squad = 'setor1'
    AND status = 'performing_atendimentos'
    AND operação IN ("operação1", "operação2")
  GROUP BY
    date,
    operação,
    squad,
    agent
),
--Explorando a base de NPS para inserção da coluna com valores médios atingidos por cada operador no dia de atuação
Nps_final AS (
  SELECT
    DATE(local_start_time) AS local_start_date,
    CAST(DATE_TRUNC(local_start_time, WEEK(SUNDAY)) AS DATE) AS week,
    local_start_month,
    selected_job_squad,
    operação,
    activity_type,
    selected_reason,
    subject_id,
    status,
    survey_nps,
    source_id,
    hsat_rating_value,
    agent,
    survey_support_evaluation,
    actor_maturity,
    squad,
    CASE WHEN survey_nps IN ("9","10") THEN 1 ELSE NULL END AS nps_value_promotoras,
    CASE WHEN survey_nps IN ("0","1","2","3","4","5","6") THEN 1 ELSE NULL END AS nps_value_detratoras,
    CASE WHEN survey_nps IN ("7","8") THEN 1 ELSE NULL END AS nps_value_neutras,
    CASE WHEN survey_nps >= "0" THEN 1 ELSE NULL END AS nps_value,
    CASE WHEN survey_nps = "1" THEN 1 ELSE NULL END AS nps_value_1,
    CASE WHEN survey_nps = "2" THEN 1 ELSE NULL END AS nps_value_2,
    CASE WHEN survey_nps = "3" THEN 1 ELSE NULL END AS nps_value_3,
    CASE WHEN survey_nps = "4" THEN 1 ELSE NULL END AS nps_value_4,
    CASE WHEN survey_nps = "5" THEN 1 ELSE NULL END AS nps_value_5,
    CASE WHEN survey_nps = "6" THEN 1 ELSE NULL END AS nps_value_6,
    CASE WHEN survey_nps = "7" THEN 1 ELSE NULL END AS nps_value_7,
    CASE WHEN survey_nps = "8" THEN 1 ELSE NULL END AS nps_value_8,
    CASE WHEN survey_nps = "9" THEN 1 ELSE NULL END AS nps_value_9,
    CASE WHEN survey_nps = "10" THEN 1 ELSE NULL END AS nps_value_10,
    DATE_DIFF(CURRENT_DATE, DATE(local_start_time), MONTH) AS M_DIFF,
    DATE_DIFF(CURRENT_DATE, DATE(local_start_time), WEEK(SUNDAY)) AS W_DIFF,
    DATE_DIFF(CURRENT_DATE, DATE(local_start_time), DAY) AS D_DIFF
  FROM `dataset-nps`
  WHERE
    squad = 'setor1'
    AND survey_nps IS NOT NULL
    AND activity_type <> 'ligações_ativas'
    AND activity_type <> 'backoffice'
    AND hsat_rating_value IS NULL
    AND local_start_month >= '2024-03-01'
    AND operação IN ('operação1', 'operação2')
)
--Definindo as colunas do banco de dados
SELECT
  job.parceira AS parceira,
  job.squad AS squad,
  job.agent AS agent,
  job.date AS date,
  job.activity_type AS activity_type,
  job.net_time_spent AS net_time_spent,
  job.total_finalizado AS total_finalizado,
  COALESCE(metrics.disponibilidade_seg, 0) AS disponibilidade_seg,
  COALESCE(metrics.disponibilidade_hora, 0) AS disponibilidade_hora,
  CASE
    WHEN metrics.disponibilidade_hora = 0 THEN 0
    ELSE job.total_finalizado / metrics.disponibilidade_hora
  END AS job_por_disponibilidade,
  
  -- Calculando o NPS em porcentagem
  
  ROUND((SUM(COALESCE(nps.nps_value_promotoras, 0)) - SUM(COALESCE(nps.nps_value_detratoras, 0))) * 100.0 / NULLIF(SUM(COALESCE(nps.nps_value, 0)), 0), 2) AS nps_value_percentage
FROM atendimentosFinalizados AS job
LEFT JOIN AgentMetrics AS metrics
ON
  job.date = metrics.date
  AND job.parceira = metrics.parceira
  AND job.squad = metrics.squad
  AND job.agent = metrics.agent
LEFT JOIN Nps_final AS nps
ON
  job.date = nps.local_start_date
  AND job.agent = nps.agent
  AND job.squad = nps.squad
WHERE
  nps.survey_nps IS NOT NULL
GROUP BY
  job.parceira,
  job.squad,
  job.agent,
  job.date,
  job.activity_type,
  job.net_time_spent,
  job.total_finalizado,
  metrics.disponibilidade_seg,
  metrics.disponibilidade_hora;
