# Vigília bounded do Project Nox

`monitor-supervisor.mjs` é o processo externo. Ele inicia um `monitor-worker.mjs`
por ciclo, acompanha o heartbeat e mata o worker quando o progresso fica sem
atualização por 10 minutos. Cinco minutos geram warning. Cada probe de banco,
HTTP e Telegram tem timeout próprio e o ciclo inteiro tem deadline.

O estado é escrito atomicamente em `vigilancia-state.json`. O contador
`effectiveSeconds` só avança quando o supervisor está vivo e o worker está
saudável ou em espera normal entre ciclos. Hangs e gaps de reinício entram em
`invalidSeconds` e não contam para as nove horas.

```sh
npm run monitor:self-test
npm run monitor:start -- --state-dir /tmp/project-nox-vigilancia-9h-final
npm run monitor:report -- --state-dir /tmp/project-nox-vigilancia-9h-final
```

O monitor é somente leitura. A coleta de métricas usa um `pg.Client` novo por
ciclo e sempre fecha o cliente em `finally`; não use `node -e` para substituir
esses scripts.

