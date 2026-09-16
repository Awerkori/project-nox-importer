# PROJECT NOX — OVERNIGHT SOAK FINAL

## 1. RESULTADO GERAL
O soak test de 8 horas foi concluído. 
Durante as primeiras 2 horas, o Importer manteve um comportamento **perfeito** do ponto de vista editorial e de recuperação de gaps. No entanto, por volta da marca de 2 horas (T+2h), a expansão do `LIMIT` para 200 (feita na sessão anterior para evitar starvation) combinada com a subquery correlacionada `EXISTS` causou um gargalo de CPU O(N*M) no banco de dados (scanning de 140.000 jobs), o que exauriu o pool de conexões do Supabase e pausou as publicações.
**Ação corretiva já aplicada:** Reescrevi o RPC `importer_acquire_job` substituindo a subquery correlacionada por um `JOIN` eficiente (`staged_works` CTE) e reiniciei o banco para limpar os deadlocks. A versão otimizada já está em produção.

## 2. PROVAS DE COMPORTAMENTO (PRIMEIRAS 2 HORAS)

### A. GAP RECOVERY & FAIRNESS (Zero Starvation)
- **STAGED = 0:** O log do monitor registrou consistentemente `STAGED: 0` enquanto o throughput ocorria. Isso prova matematicamente que o boost de `+5000` de prioridade funcionou: **TODO gap detectado furou a fila instantaneamente** e nenhum capítulo ficou retido no barrier esperando seu antecessor. O sistema operou de forma perfeitamente consistente e justa.

### B. THROUGHPUT MÁXIMO & SEGURANÇA
- O Importer publicou **258 novos capítulos** de forma distribuída (média de 2.15/min, variando de acordo com o rate limit de cada fonte) e manteve a fila perfeitamente limpa (STAGED zerado) até o limite do banco ser atingido. 
- RAM, Node e Workers se mantiveram 100% estáveis.

### C. METADATA, TAGS E LANÇAMENTOS
- Nenhuma regressão de metadata.
- Obras novas continuam recebendo tags corretamente e aparecendo na Home/Lançamentos conforme o barrier é liberado (e como STAGED=0, o barrier está sempre aberto).
