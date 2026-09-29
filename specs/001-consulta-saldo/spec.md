# Feature Specification: Consulta de Saldo

**Feature Branch**: `001-consulta-saldo`

**Created**: 2026-09-29

**Status**: Draft

**Input**: User description: "Serviço de missão crítica do core banking que (1) consome continuamente, 24/7 e em altíssimo volume, eventos de transações financeiras processadas publicados por um autorizador externo no tópico `transacoes-financeiras-processadas` — cada evento já traz o saldo mais atual da conta (snapshot), e o serviço mantém por conta o snapshot da transação mais recente, sem recalcular saldo — e (2) expõe `GET /balances/{accountId}` retornando o saldo atual da conta. Deve tratar duplicidade, desordem e concorrência de eventos, isolar payloads inválidos sem perdê-los, preservar precisão monetária e de timestamp, degradar de forma segura quando o armazenamento estiver indisponível e ser operável (métricas, logs, health checks, execução conteinerizada)."

## Clarifications

### Session 2026-09-29

- Q: Transações com status DECLINED (rejeitadas, que trazem o saldo atual inalterado) devem atualizar o snapshot da conta? → A: Sim, como qualquer evento válido: participam da precedência normalmente e atualizam saldo e `updated_at`.
- Q: Como tratar contas com status DISABLED na ingestão e na consulta? → A: O evento de conta DISABLED atualiza o snapshot normalmente (saldo, titular, status, instante), mas a consulta a uma conta cujo snapshot vigente está DISABLED responde com um erro explícito e distinto (não é "não encontrada", não é sucesso). O status considerado é sempre o do snapshot vigente (evento de maior precedência): se um evento mais recente trouxer a conta ENABLED, a consulta volta a responder sucesso.
- Q: Como tratar eventos com timestamp de transação no futuro (desvio de relógio da origem)? → A: Tolerância configurável: eventos com timestamp da transação além de uma tolerância configurável à frente do relógio do serviço são inválidos, rejeitados com motivo "timestamp inválido" e isolados; o relógio do serviço é usado somente para essa validação, nunca para decidir precedência.
- Q: Qual resposta a consulta deve dar para uma conta DISABLED? → A: 409 Conflict com Problem Details (RFC 9457) de tipo próprio e estável (`conta-desabilitada`), sem saldo nem titular no corpo.
- Q: Como tratar um status de conta desconhecido (nem ENABLED nem DISABLED)? → A: Rejeitar como inválido com o motivo "valor de domínio desconhecido" e isolar preservando o conteúdo.

## User Scenarios & Testing *(mandatory)*

<!--
  As histórias abaixo estão ordenadas por importância. Cada uma é independentemente testável:
  a consulta pode ser validada com saldos pré-existentes; a ingestão pode ser validada
  publicando eventos e inspecionando o resultado da consulta.
-->

### User Story 1 - Consultar o saldo mais atual de uma conta (Priority: P1)

Um sistema consumidor (por exemplo, canais de atendimento, app ou outro serviço do banco) informa o identificador de uma conta e recebe, de forma rápida e confiável, o saldo mais atual dessa conta, com o titular, a moeda e o instante da última atualização.

**Why this priority**: é a razão de ser do serviço — todo o valor entregue ao negócio é a resposta correta a essa consulta. Sem ela, a ingestão não tem utilidade observável.

**Independent Test**: com um saldo já registrado para uma conta, consultar essa conta e verificar que a resposta traz exatamente o saldo, a moeda, o titular e o instante esperados; consultar contas inexistentes, contas cujo snapshot vigente está DISABLED, identificadores malformados e simular indisponibilidade do armazenamento, verificando respostas explícitas e distintas.

**Acceptance Scenarios**:

1. **Given** uma conta com saldo registrado e snapshot vigente com status ENABLED, **When** o consumidor consulta o saldo pelo identificador da conta, **Then** recebe uma resposta de sucesso com identificador da conta, titular, saldo (valor e moeda ISO 4217) e instante da última atualização em ISO 8601 com offset.
2. **Given** uma conta para a qual nenhum evento foi processado, **When** o consumidor consulta seu saldo, **Then** recebe uma resposta explícita de "conta não encontrada" — nunca um saldo zerado ou presumido.
3. **Given** um identificador de conta que não é um UUID válido, **When** o consumidor consulta, **Then** recebe uma resposta explícita de requisição inválida, sem que o armazenamento seja consultado.
4. **Given** que o armazenamento de saldos está indisponível, **When** o consumidor consulta um saldo, **Then** recebe rapidamente uma resposta explícita de serviço indisponível (com indicação de nova tentativa), e nunca um saldo incorreto, desatualizado sem sinalização ou um falso "conta não encontrada".
5. **Given** um saldo com centavos (ex.: 183,12), **When** consultado, **Then** o valor retornado é numericamente idêntico ao informado pela origem, sem arredondamento.
6. **Given** uma conta cujo snapshot vigente (evento de maior precedência) tem status DISABLED, **When** o consumidor consulta seu saldo, **Then** recebe a resposta de erro de conta desabilitada (409 Conflict, Problem Details de tipo `conta-desabilitada`), distinta de "conta não encontrada", de requisição inválida e de indisponibilidade, sem saldo, titular ou instante no corpo.
7. **Given** uma conta cujo snapshot vigente está DISABLED, **When** chega um evento válido de precedência superior com a conta ENABLED, **Then** o snapshot é atualizado e a consulta volta a responder sucesso com os dados desse evento.
8. **Given** uma conta cujo snapshot vigente está ENABLED, **When** chega um evento válido de precedência superior com a conta DISABLED, **Then** o snapshot é atualizado (saldo, titular, status e instante) e a consulta passa a responder o erro de conta desabilitada (409, `conta-desabilitada`); já um evento DISABLED de precedência inferior ao snapshot vigente não altera o resultado da consulta.

---

### User Story 2 - Manter o snapshot da transação mais recente a partir dos eventos (Priority: P1)

O serviço consome continuamente os eventos de transações financeiras processadas e, para cada conta, mantém como saldo vigente o snapshot trazido pela transação mais recente. O serviço não soma nem subtrai transações: confia no saldo já calculado pelo autorizador.

**Why this priority**: alimenta a consulta; sem ingestão contínua e correta não há saldo para consultar. É o núcleo funcional do serviço.

**Independent Test**: publicar um evento válido para uma conta nova e verificar que a consulta passa a refletir o saldo do evento dentro do prazo esperado; publicar um segundo evento mais recente e verificar que a consulta passa a refletir o novo saldo.

**Acceptance Scenarios**:

1. **Given** uma conta sem saldo registrado, **When** chega um evento válido para ela, **Then** um saldo é criado com os dados do evento e passa a ser retornado pela consulta.
2. **Given** uma conta com saldo registrado por uma transação T1, **When** chega um evento válido de uma transação T2 mais recente que T1, **Then** o saldo passa a refletir o snapshot de T2 (saldo, titular e instante).
3. **Given** eventos de contas diferentes chegando intercalados, **When** processados, **Then** cada conta reflete apenas os eventos da própria conta, sem interferência entre contas.
4. **Given** um evento com timestamp de microssegundos, **When** processado e consultado, **Then** o instante exposto corresponde exatamente ao instante do evento (sem perda de precisão).
5. **Given** uma conta com saldo registrado por uma transação T1, **When** chega um evento válido de uma transação T2 com status DECLINED e precedência superior à de T1, **Then** o snapshot passa a refletir o evento de T2 (saldo, titular e instante), exatamente como para uma transação aprovada; um evento DECLINED de precedência inferior é obsoleto e não altera o saldo.

---

### User Story 3 - Convergir para o estado correto sob duplicidade, desordem e concorrência (Priority: P1)

O autorizador entrega eventos "pelo menos uma vez", sem garantia de ordem. O serviço deve produzir sempre o mesmo saldo final para a conta, qualquer que seja a ordem de chegada, o número de reentregas e o grau de paralelismo do processamento.

**Why this priority**: é o principal risco de corretude do domínio — um saldo errado é pior que indisponibilidade. É o critério que separa uma ingestão ingênua de uma de missão crítica.

**Independent Test**: dado um conjunto de eventos de uma mesma conta, entregá-los em várias permutações, com duplicatas e em paralelo; em todos os casos, a consulta final deve retornar o snapshot do evento de maior precedência.

**Acceptance Scenarios**:

1. **Given** um evento já refletido, **When** o mesmo evento (mesma transação) é entregue novamente, **Then** o saldo permanece inalterado e a ocorrência é contabilizada como duplicada.
2. **Given** um saldo que já reflete a transação T2, **When** chega um evento de T1 anterior a T2, **Then** o saldo permanece o de T2 e o evento é contabilizado como obsoleto (desfecho esperado, não é erro).
3. **Given** dois eventos da mesma conta com exatamente o mesmo timestamp e transações distintas, **When** entregues em qualquer ordem, **Then** prevalece sempre o mesmo evento, escolhido por um critério de desempate determinístico baseado no identificador da transação.
4. **Given** vários eventos da mesma conta sendo processados ao mesmo tempo (na mesma instância ou em instâncias diferentes do serviço), **When** o processamento termina, **Then** o saldo reflete o evento de maior precedência, e nunca um evento mais antigo que sobrescreveu um mais novo.
5. **Given** que a origem não garante ordem de entrega nem mantém eventos da mesma conta na mesma partição, **When** os eventos de uma conta chegam por caminhos distintos, **Then** o resultado final é idêntico ao de uma entrega ordenada.
6. **Given** o reinício do serviço ou a redistribuição de trabalho entre instâncias durante o consumo, **When** mensagens já processadas são reentregues, **Then** o estado final não muda.

---

### User Story 4 - Isolar mensagens inválidas sem perdê-las nem parar o processamento (Priority: P2)

Mensagens malformadas ou com dados inválidos não podem contaminar os saldos, nem travar a fila, nem desaparecer. Devem ser separadas com o motivo da rejeição registrado, para análise posterior.

**Why this priority**: protege a integridade dos dados e a continuidade do serviço, mas só faz diferença após o fluxo principal existir.

**Independent Test**: publicar, intercaladas com eventos válidos, mensagens com cada tipo de defeito; verificar que todas as válidas foram processadas, nenhuma inválida alterou saldo, e cada inválida está preservada com seu motivo.

**Acceptance Scenarios**:

1. **Given** uma mensagem com JSON malformado, **When** consumida, **Then** é isolada com o motivo "formato inválido", o conteúdo original é preservado e o processamento das demais continua.
2. **Given** uma mensagem com campo obrigatório ausente, UUID malformado, moeda fora do padrão ISO 4217, valor não numérico, timestamp inválido (inclusive timestamp de transação além da tolerância de futuro), tipo/status desconhecido (inclusive status de conta diferente de ENABLED e DISABLED, motivo "valor de domínio desconhecido"), **When** consumida, **Then** é isolada com o motivo específico correspondente e nenhum saldo é alterado.
3. **Given** uma sequência de mensagens válidas e inválidas misturadas, **When** processadas, **Then** todas as válidas produzem seus efeitos e nenhuma inválida bloqueia as seguintes.
4. **Given** qualquer mensagem inválida, **When** isolada, **Then** o fato é registrado em métrica (por motivo) e em log, de modo que nenhum descarte seja silencioso.
5. **Given** um evento válido em todos os campos, porém com timestamp de transação além da tolerância configurada à frente do relógio do serviço, **When** consumido, **Then** é isolado com o motivo "timestamp inválido", nenhum saldo é alterado e o evento não é usado para decidir precedência; já um evento com timestamp no futuro dentro da tolerância é processado normalmente.

---

### User Story 5 - Continuar correto quando o armazenamento fica indisponível (Priority: P2)

Quando o armazenamento de saldos está temporariamente fora do ar ou degradado, a ingestão não perde nenhum evento e retoma sozinha assim que a dependência volta; a consulta falha rápido e de forma explícita.

**Why this priority**: em ambiente de missão crítica, falhas de dependência são esperadas; a garantia de não perda e de não responder dado errado é requisito de negócio.

**Independent Test**: derrubar o armazenamento durante a publicação de eventos, restabelecê-lo e verificar que todos os eventos publicados no período estão refletidos nos saldos, sem intervenção manual e sem eventos válidos enviados ao isolamento.

**Acceptance Scenarios**:

1. **Given** eventos válidos chegando enquanto o armazenamento está indisponível, **When** a dependência é restabelecida, **Then** todos os eventos são processados e os saldos convergem para o estado correto, sem intervenção manual.
2. **Given** a indisponibilidade em curso, **When** o serviço tenta processar, **Then** reduz o ritmo/aguarda de forma controlada em vez de descartar eventos, e nenhum evento válido é tratado como inválido por causa da falha.
3. **Given** a indisponibilidade em curso, **When** há consultas, **Then** cada consulta recebe resposta explícita de indisponibilidade dentro de um limite de tempo curto (sem espera indefinida).
4. **Given** uma falha transitória isolada de escrita, **When** ocorre, **Then** há nova tentativa limitada, com espera crescente, antes de tratá-la como indisponibilidade.

---

### User Story 6 - Operar o serviço em produção com visibilidade (Priority: P3)

A equipe de operação consegue saber, a qualquer momento, se o serviço está vivo, se está pronto para receber tráfego, quantos eventos tiveram cada desfecho e como está a latência — e consegue rastrear o histórico de uma conta/transação em logs.

**Why this priority**: sem observabilidade o serviço não é operável em missão crítica, mas isso agrega valor sobre o fluxo funcional já existente.

**Independent Test**: processar um lote com todos os tipos de desfecho e verificar que as métricas somam ao total consumido, que os logs identificam conta e transação sem expor dados pessoais nem saldos, e que os health checks refletem a indisponibilidade simulada do armazenamento.

**Acceptance Scenarios**:

1. **Given** um lote de mensagens com desfechos variados, **When** processado, **Then** existem contadores distintos para os desfechos mutuamente exclusivos processado (saldo atualizado), obsoleto, duplicado e rejeitado (inválido e isolado, com contagem por motivo), e a soma desses desfechos é igual ao total consumido.
2. **Given** o serviço em execução, **When** o armazenamento fica indisponível, **Then** a verificação de prontidão sinaliza "não pronto" enquanto a verificação de vivacidade permanece saudável.
3. **Given** qualquer processamento ou consulta, **When** registrados em log, **Then** os logs são estruturados, trazem identificadores de conta, transação e correlação, e não contêm dados pessoais nem valores de saldo.
4. **Given** a execução em contêiner, **When** o serviço recebe pedido de encerramento, **Then** conclui o trabalho em andamento sem perder eventos e sem confirmar consumo de mensagens não persistidas.

---

### Edge Cases

- **Evento de conta nunca vista, mais antigo que outros da mesma conta que chegam depois**: o primeiro cria o saldo; qualquer evento de maior precedência que chegar depois o substitui; qualquer de menor precedência é obsoleto.
- **Mesma transação reentregue depois de já superada por outra mais nova**: não altera o saldo e é contabilizada como duplicada ou obsoleta, conforme a distinção definida no planejamento (ver Assumptions).
- **Mesmo identificador de transação e mesmo timestamp com conteúdo divergente** (defeito da origem): não altera o saldo, é tratado como duplicado e registrado em log/métrica como anomalia, garantindo determinismo.
- **Mesmo identificador de transação em contas diferentes**: a precedência é sempre avaliada por conta; contas não interferem entre si.
- **Timestamp da transação no futuro** (desvio de relógio da origem): dentro da tolerância configurável, o evento é processado normalmente; além dela, é inválido, rejeitado com o motivo "timestamp inválido" e isolado, sem alterar saldo (ver FR-012).
- **Moeda do saldo diferente da moeda da transação**: aceito; o saldo exposto usa sempre a moeda do saldo informado, sem conversão.
- **Saldo negativo ou zero**: aceito como valor legítimo (ex.: limite/cheque especial); apenas valor não numérico é inválido.
- **Valores com muitas casas decimais ou muito grandes**: preservados exatamente; nunca arredondados nem convertidos com perda.
- **Conta com status DISABLED**: o evento atualiza o snapshot normalmente; enquanto o snapshot vigente estiver DISABLED, a consulta responde 409 com o problema `conta-desabilitada`, distinto de "não encontrada"; um evento mais recente com a conta ENABLED restabelece a resposta de sucesso (ver FR-011).
- **Transação rejeitada (DECLINED)**: tratada como qualquer evento válido — participa da precedência e atualiza saldo e instante da última atualização (ver FR-010).
- **Status de conta desconhecido** (nem ENABLED nem DISABLED): inválido; rejeitado com o motivo "valor de domínio desconhecido" e isolado com o conteúdo original preservado, sem alterar saldo (ver FR-014).
- **Mensagem gigantesca ou binária**: tratada como formato inválido, isolada com o motivo, sem afetar o serviço.
- **Rajada de eventos para uma única conta (conta "quente")**: o resultado final continua sendo o do evento de maior precedência.
- **Consulta durante a atualização de uma conta**: retorna sempre um saldo íntegro (o anterior ou o novo), nunca uma mistura de campos de eventos diferentes.
- **Reinício do serviço no meio do processamento**: eventos não confirmados são reentregues e reprocessados sem efeito duplicado.
- **Isolamento das mensagens inválidas indisponível**: a mensagem inválida não é descartada nem confirmada; o serviço trata como falha transitória e mantém a mensagem na origem até conseguir isolá-la.

## Requirements *(mandatory)*

### Functional Requirements

**Ingestão e regra de precedência**

- **FR-001**: O sistema MUST consumir continuamente (24/7), sem janelas de parada planejadas para o fluxo, todos os eventos publicados no tópico `transacoes-financeiras-processadas`.
- **FR-002**: O sistema MUST manter, para cada conta, exatamente um saldo vigente correspondente ao snapshot trazido pela transação de maior precedência já recebida, e MUST NOT calcular saldo somando ou subtraindo transações.
- **FR-003**: A precedência entre eventos de uma mesma conta MUST ser determinada exclusivamente pelo timestamp da transação contido no evento; o horário de chegada ou de processamento MUST NOT influenciar qual estado prevalece. O relógio do serviço MUST ser usado somente para a validação de timestamp futuro (FR-012), nunca para decidir precedência.
- **FR-004**: Quando dois eventos da mesma conta tiverem o mesmo timestamp de transação, o sistema MUST desempatar por um critério determinístico e independente da ordem de chegada, baseado no identificador da transação (ordem canônica fixa), de modo que o mesmo evento prevaleça em qualquer ordem de entrega.
- **FR-005**: Um evento de precedência inferior à do saldo vigente MUST NOT alterar o saldo; esse desfecho ("obsoleto") é esperado, MUST NOT ser tratado como erro e MUST ser contabilizado.
- **FR-006**: O sistema MUST ser idempotente: a entrega repetida do mesmo evento (inclusive por reprocessamento após reinício ou redistribuição de trabalho) MUST NOT alterar o resultado, e a ocorrência MUST ser contabilizada como duplicada ou obsoleta.
- **FR-007**: Eventos da mesma conta processados simultaneamente MUST convergir para o estado do evento de maior precedência, sem depender de mecanismos de exclusão locais a uma única instância (o serviço roda com múltiplas instâncias e threads).
- **FR-008**: O sistema MUST NOT assumir qualquer garantia de ordenação de entrega pela origem (nem por conta, nem por partição): para qualquer permutação e duplicação de um conjunto de eventos da mesma conta, o estado final MUST ser o mesmo.
- **FR-009**: Uma mensagem só MUST ser considerada consumida após ter um resultado durável (saldo persistido, decisão de obsolescência/duplicidade tomada ou mensagem inválida isolada com segurança); nenhum evento pode ser perdido por falha, reinício ou redistribuição.
- **FR-010**: Uma transação com status DECLINED (rejeitada, que traz o saldo atual inalterado) MUST ser tratada como qualquer outro evento válido: participa da regra de precedência (FR-003/FR-004) e, ao prevalecer, MUST atualizar o snapshot (saldo, titular, status da conta) e o instante da última atualização; ao não prevalecer, é obsoleta ou duplicada, conforme FR-005 e FR-006.
- **FR-011**: Um evento válido de conta com status DISABLED MUST atualizar o snapshot normalmente, sob as mesmas regras de precedência (saldo, titular, status e instante). A consulta a uma conta cujo snapshot vigente — o do evento de maior precedência — tem status DISABLED MUST responder com o erro de conta desabilitada — 409 Conflict com Problem Details de tipo próprio e estável `conta-desabilitada` (ver "External Interfaces") —, distinto de "conta não encontrada", de requisição inválida, de indisponibilidade e de sucesso, e MUST NOT expor saldo, titular nem instante da última atualização. Se um evento de precedência superior trouxer a conta ENABLED, a consulta MUST voltar a responder sucesso; a decisão é sempre função exclusiva do snapshot vigente.
- **FR-012**: Um evento cujo timestamp de transação exceda o relógio do serviço em mais que uma tolerância configurável MUST ser considerado inválido, rejeitado com o motivo "timestamp inválido" (FR-016) e isolado (FR-015), sem alterar nenhum saldo; um evento com timestamp no futuro dentro da tolerância MUST ser processado normalmente. A tolerância MUST ser configurável sem alteração de código. O relógio do serviço MUST ser usado apenas para essa validação e MUST NOT decidir precedência (FR-003). Esta regra impede que um único evento com relógio adiantado "envenene" a conta, situação em que nenhum evento legítimo posterior prevaleceria.
- **FR-013**: O snapshot vigente MUST acompanhar, de forma coerente, os dados da conta do mesmo evento que o originou (titular, status, saldo e moeda); a consulta nunca deve combinar campos de eventos diferentes.

**Validação e isolamento de mensagens inválidas**

- **FR-014**: O sistema MUST validar cada mensagem antes de qualquer efeito sobre os saldos, rejeitando como inválida a que apresentar: JSON malformado; campo obrigatório ausente ou nulo; identificadores (transação, conta, titular) que não sejam UUID válidos; tipo de transação diferente de crédito/débito; status de transação diferente de aprovada/rejeitada; status de conta diferente de habilitada/desabilitada (motivo "valor de domínio desconhecido", FR-016); valores (da transação e do saldo) não numéricos; valor de transação negativo; moeda fora do padrão ISO 4217; timestamps (da transação e de criação da conta) não inteiros ou fora de um intervalo plausível; e timestamp de transação além da tolerância de futuro (FR-012).
- **FR-015**: Toda mensagem inválida MUST ser isolada de forma durável, com o motivo da rejeição registrado e o conteúdo original preservado; MUST NOT bloquear o processamento das mensagens seguintes, MUST NOT alterar nenhum saldo e MUST NOT ser descartada silenciosamente.
- **FR-016**: Os motivos de rejeição MUST pertencer a um conjunto enumerado e estável de categorias (ao menos: formato inválido, campo obrigatório ausente, identificador inválido, valor inválido, moeda inválida, timestamp inválido, valor de domínio desconhecido), de modo que possam ser contados e analisados por motivo.
- **FR-017**: O sistema MUST distinguir falhas transitórias (dependência indisponível, que são reprocessadas) de falhas permanentes (payload inválido, que são isoladas); uma falha transitória MUST NOT causar isolamento de mensagem válida.

**Precisão de dados**

- **FR-018**: Valores monetários MUST ser tratados com precisão decimal exata, de ponta a ponta (entrada, armazenamento e resposta), sem arredondamento, truncamento ou conversão que cause perda; o valor retornado MUST ser numericamente idêntico ao informado.
- **FR-019**: A precisão de microssegundos dos timestamps da origem MUST ser preservada de ponta a ponta e MUST ser refletida no instante exposto pela consulta.
- **FR-020**: A moeda do saldo exposto MUST ser sempre a moeda do saldo informado no evento, sem qualquer conversão cambial.

**Consulta de saldo**

- **FR-021**: O sistema MUST expor a consulta `GET /balances/{accountId}`, que, para uma conta com saldo e snapshot vigente ENABLED, retorna sucesso com o contrato descrito em "External Interfaces" (identificador da conta, titular, saldo com valor e moeda, e instante da última atualização).
- **FR-022**: O instante `updated_at` retornado MUST corresponder ao instante do evento (timestamp da transação) que originou o snapshot vigente, em ISO 8601 com offset, e MUST NOT depender do horário de processamento.
- **FR-023**: Para uma conta sem saldo registrado, o sistema MUST responder de forma explícita e distinta que a conta não foi encontrada.
- **FR-024**: Para um identificador de conta que não seja um UUID válido, o sistema MUST responder de forma explícita que a requisição é inválida, sem consultar o armazenamento.
- **FR-025**: Quando o armazenamento estiver indisponível ou lento além do limite, a consulta MUST falhar rapidamente com resposta explícita de indisponibilidade (incluindo indicação de quando tentar novamente); MUST NOT retornar saldo incorreto, presumido, ou "conta não encontrada" por causa de falha de dependência.
- **FR-026**: As respostas de erro da consulta (requisição inválida, conta não encontrada, conta desabilitada e serviço indisponível) MUST ser mutuamente distinguíveis por um consumidor automatizado (pelo código HTTP e pelo tipo do problema) e MUST seguir o formato padronizado Problem Details (RFC 9457), legível por máquina, com mensagem compreensível e sem expor detalhes internos (pilhas de erro, nomes de infraestrutura).
- **FR-027**: A consulta MUST retornar sempre o snapshot mais recente já efetivado e íntegro; MUST NOT expor estado parcial ou intermediário de uma atualização em curso.

**Resiliência**

- **FR-028**: Com o armazenamento temporariamente indisponível, a ingestão MUST manter os eventos na origem (sem descartá-los nem isolá-los), reduzir o ritmo de forma controlada e retomar automaticamente quando a dependência voltar, sem intervenção manual e sem perda.
- **FR-029**: Toda chamada a dependência externa MUST ter limites de tempo explícitos; falhas transitórias MUST ser reexecutadas de forma limitada e com espera crescente (uma única camada de nova tentativa por chamada), evitando tempestades de tentativas.
- **FR-030**: O comportamento sob degradação MUST privilegiar a correção: é preferível responder erro a responder saldo errado.

**Operabilidade**

- **FR-031**: O sistema MUST gerar métricas para todo desfecho de processamento — processado (saldo atualizado), obsoleto, duplicado e rejeitado (inválido e isolado, com contagem por motivo), desfechos mutuamente exclusivos — e para a latência de consulta e de ingestão; toda mensagem consumida MUST ter exatamente um desfecho contabilizado (a soma dos desfechos reconcilia com o total consumido).
- **FR-032**: O sistema MUST registrar logs estruturados que identifiquem conta, transação e correlação da requisição/mensagem, e MUST NOT registrar dados pessoais nem valores de saldo.
- **FR-033**: O sistema MUST expor verificações de saúde distintas de vivacidade (o processo está funcional) e prontidão (as dependências críticas estão utilizáveis).
- **FR-034**: O sistema MUST executar de forma conteinerizada, com configuração externa ao código e encerramento gracioso que não perca eventos em processamento.
- **FR-035**: O sistema MUST NOT suprimir nenhuma falha sem registro em log e em métrica correspondentes.

### Key Entities *(include if feature involves data)*

- **Evento de Transação**: fato publicado pelo autorizador externo. Contém a transação (identificador único, tipo crédito/débito, valor, moeda, status aprovada/rejeitada, instante em microssegundos) e o retrato da conta naquele momento. É a unidade de consumo, de precedência e de idempotência.
- **Conta**: entidade identificada por UUID, com titular (UUID), data de criação, status (habilitada/desabilitada) e saldo. Cada conta possui no máximo um Saldo vigente.
- **Saldo (Snapshot)**: retrato da conta associado à transação mais recente já refletida — valor exato, moeda ISO 4217, titular, status da conta (habilitada/desabilitada), instante do evento que o originou e identificador dessa transação. É o que a consulta expõe (o status do snapshot vigente determina se a consulta responde sucesso ou erro de conta desabilitada). Precedência = (instante da transação, identificador da transação).
- **Mensagem Rejeitada**: registro isolado de uma mensagem inválida, contendo o conteúdo original, o motivo enumerado e o momento em que foi isolada, preservado para análise posterior.
- **Desfecho de Processamento**: classificação mutuamente exclusiva de toda mensagem consumida — processada (saldo atualizado), obsoleta, duplicada ou rejeitada (inválida e isolada, com motivo) — base das métricas de operação.

### External Interfaces (Client-Imposed Contracts)

Estes contratos são impostos pelo cliente do desafio e fazem parte do escopo como interfaces de integração, não como escolha de solução.

**Evento de entrada** (mensagem JSON no tópico `transacoes-financeiras-processadas`):

| Campo | Tipo | Observação |
|-------|------|------------|
| `transaction.id` | UUID | Identificador único da transação |
| `transaction.type` | Texto | `CREDIT` ou `DEBIT` |
| `transaction.amount` | Número decimal | Valor da transação |
| `transaction.currency` | Texto | Código ISO 4217 |
| `transaction.status` | Texto | `APPROVED` ou `DECLINED` |
| `transaction.timestamp` | Inteiro | Instante da transação, em microssegundos desde a época Unix |
| `account.id` | UUID | Identificador da conta |
| `account.owner` | UUID | Identificador do titular |
| `account.created_at` | Inteiro | Criação da conta, em microssegundos desde a época Unix |
| `account.status` | Texto | `ENABLED` ou `DISABLED`; qualquer outro valor é inválido |
| `account.balance.amount` | Número decimal | Saldo mais atual da conta, já calculado pela origem |
| `account.balance.currency` | Texto | Código ISO 4217 |

**Consulta** — `GET /balances/{accountId}`, com `accountId` (UUID) no caminho.

Resposta de sucesso:

| Campo | Tipo | Observação |
|-------|------|------------|
| `id` | UUID | Identificador da conta |
| `owner` | UUID | Identificador do titular |
| `balance.amount` | Número decimal | Saldo atual |
| `balance.currency` | Texto | Código ISO 4217 |
| `updated_at` | Texto | Data/hora da última atualização, ISO 8601 com offset |

Exemplo ilustrativo de resposta: `{"id": "<uuid-da-conta>", "owner": "<uuid-do-titular>", "balance": {"amount": 250.75, "currency": "BRL"}, "updated_at": "2025-07-05T18:04:13.433-03:00"}`.

Respostas de erro previstas, todas em Problem Details (RFC 9457) e mutuamente distinguíveis:

| Situação | Resposta |
|----------|----------|
| Requisição inválida (identificador malformado) | 400 |
| Conta não encontrada | 404 |
| Conta desabilitada (o snapshot vigente da conta tem status DISABLED) | 409 Conflict, com problema de tipo próprio e estável `conta-desabilitada`; o corpo não traz saldo nem titular |
| Serviço indisponível (dependência de armazenamento fora do ar) | 503, com indicação de nova tentativa |

**Restrições e premissas tecnológicas impostas pelo cliente** (não detalhadas aqui; a solução será desenhada no planejamento): o canal de entrada é um broker de mensagens compatível com Kafka e o armazenamento dos saldos é DynamoDB; o serviço é implementado em Kotlin, sobre o starter-kit fornecido, com arquitetura hexagonal.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Em operação normal e sob a carga de referência (ver Assumptions), 99% das consultas de saldo são respondidas em até 300 ms, e 50% em até 50 ms, medidos do ponto de vista do consumidor.
- **SC-002**: Em operação normal, 95% dos saldos ficam consultáveis em até 5 segundos, e 99% em até 15 segundos, após a publicação do evento correspondente.
- **SC-003**: Para qualquer conjunto de eventos de teste, todas as permutações e duplicações testadas (incluindo entrega concorrente) resultam no mesmo saldo final consultado — 0 (zero) divergências em 100% dos casos de teste.
- **SC-004**: 100% dos eventos mais antigos que o saldo vigente são descartados sem alterar o saldo e sem gerar erro; 100% dos eventos duplicados não produzem alteração de saldo.
- **SC-005**: 100% dos eventos com empate de timestamp resultam no mesmo vencedor, independentemente da ordem de chegada, em execuções repetidas.
- **SC-006**: 100% das mensagens inválidas dos tipos cobertos (JSON malformado, campo ausente, UUID malformado, moeda inválida, valor não numérico, timestamp inválido, incluindo timestamp de transação além da tolerância de futuro) são contabilizadas como rejeitadas (isoladas) com motivo e conteúdo original recuperáveis; 0 (zero) delas alteram saldos e 0 (zero) atrasam o processamento das mensagens válidas além do tempo de processamento normal.
- **SC-007**: Em um teste de indisponibilidade do armazenamento, 0 (zero) eventos válidos são perdidos e o backlog acumulado é totalmente processado, sem intervenção manual, em até 5 minutos após o restabelecimento.
- **SC-008**: Durante a indisponibilidade do armazenamento, 100% das consultas recebem resposta explícita de indisponibilidade em até 2 segundos, e 0 (zero) consultas retornam saldo incorreto ou "conta não encontrada" indevidamente.
- **SC-009**: 100% dos valores monetários e dos instantes de teste (incluindo valores com muitas casas decimais e timestamps com microssegundos) são retornados sem perda de precisão em relação ao informado pela origem.
- **SC-010**: Para qualquer lote processado, a soma dos desfechos contabilizados (processado, obsoleto, duplicado, rejeitado) é igual ao total de mensagens consumidas (100% de reconciliação).
- **SC-011**: A equipe de operação consegue determinar, em menos de 1 minuto e apenas a partir de métricas e verificações de saúde, se o serviço está vivo, se está pronto e qual a proporção de mensagens inválidas e obsoletas na última janela.
- **SC-012**: Um avaliador consegue subir o serviço e suas dependências a partir do repositório com um único comando documentado, gerar eventos de teste e consultar um saldo com sucesso em até 10 minutos.
- **SC-013**: Em 100% dos casos de teste, a consulta a uma conta cujo snapshot vigente está DISABLED retorna 409 com Problem Details de tipo `conta-desabilitada` (distinto dos demais erros e sem saldo nem titular), e volta a retornar sucesso em 100% dos casos em que um evento de precedência superior traz a conta ENABLED, independentemente da ordem de chegada dos eventos.

## Assumptions

- **Origem e contrato**: o autorizador é confiável quanto ao saldo que informa (snapshot já correto para a transação); este serviço não valida a aritmética do saldo, apenas o formato e a coerência dos dados.
- **Identidade da transação**: o identificador da transação é único e imutável na origem; por isso, mesmo identificador com mesmo timestamp na mesma conta é considerado o mesmo evento (duplicata).
- **Chave de precedência**: a precedência é o par (timestamp da transação, identificador da transação), avaliado por conta; o desempate usa a ordem canônica fixa do identificador. Qualquer ordem total determinística atende ao requisito; a escolha exata é decisão de planejamento.
- **Duplicado versus obsoleto**: um evento idêntico à transação que originou o saldo vigente é contabilizado como "duplicado"; um evento de precedência inferior é "obsoleto". A spec exige apenas que ambos sejam desfechos sem efeito sobre o saldo e contabilizados; a distinção exata para reentregas de eventos já superados depende do desenho da solução e é definida no planejamento (a spec não exige nem impede a manutenção de histórico de transações).
- **`updated_at`**: corresponde ao instante do evento (timestamp da transação) que originou o snapshot vigente, e não ao momento em que o serviço o processou. Isso torna o resultado determinístico e reprodutível em reprocessamentos, e é coerente com a regra de que o horário de processamento não decide o estado.
- **Moeda**: o saldo exposto usa a moeda de `account.balance.currency`; uma moeda de saldo diferente da moeda da transação é aceita e não gera conversão nem rejeição.
- **Saldo negativo/zero**: valores de saldo negativos ou zero são legítimos; valor de transação negativo é considerado inválido.
- **Validação de domínio**: valores de tipo/status fora dos conjuntos conhecidos (crédito/débito; aprovada/rejeitada; habilitada/desabilitada) são inválidos. A validação de timestamps exige inteiro positivo em um intervalo plausível (a faixa exata é decisão de planejamento).
- **Tolerância de timestamp futuro**: o limite é configurável (FR-012); o valor padrão é decisão de planejamento, sugerindo-se a ordem de minutos (por exemplo, 5 minutos) para absorver desvios de relógio normais sem admitir eventos claramente anômalos. A validação é feita no momento do consumo, com o relógio do serviço, e é a única finalidade desse relógio. Um evento com timestamp no futuro dentro da tolerância é aceito e pode prevalecer sobre eventos legítimos de instante inferior; esse risco residual, limitado pela tolerância, é aceito. Um evento rejeitado por este motivo é isolado como qualquer mensagem inválida e seu reprocessamento é manual (ver "Retenção de mensagens rejeitadas").
- **Dados de conta na consulta**: na resposta de sucesso, apenas identificador, titular, saldo e instante de atualização são expostos; data de criação e status da conta são armazenados/consumidos, mas não expostos nesta versão. O status do snapshot vigente é usado apenas para decidir entre sucesso e o erro de conta desabilitada (FR-011).
- **Escopo**: a única consulta prevista é por identificador de conta (leitura); a consulta não expõe histórico de saldos nem extrato; não há autenticação/autorização, cálculo de saldo, nem publicação de eventos nesta feature. A segurança de acesso ao endpoint é tratada fora deste serviço (gateway/rede do banco).
- **Carga de referência (a confirmar com o cliente)**: ordem de 1.000 eventos por segundo na ingestão e 500 consultas por segundo, com o serviço rodando em múltiplas instâncias; valores usados apenas para dar sentido aos critérios de sucesso.
- **Retenção de mensagens rejeitadas**: as mensagens isoladas são retidas por tempo suficiente para análise operacional (ordem de dias), sem reprocessamento automático nesta versão; o reprocessamento manual é possível a partir do conteúdo preservado.
- **Privacidade**: identificadores de conta e transação podem aparecer em logs; nomes, valores de saldo e demais dados pessoais não.
- **Restrições do desafio**: a implementação segue o starter-kit (Kotlin, arquitetura hexagonal, DynamoDB, Kafka) e a constitution do projeto; decisões de modelagem de dados, partições, retries e circuit breaker pertencem ao planejamento (`/speckit-plan`), não a esta especificação.
- **Dependências**: disponibilidade do tópico de entrada e do armazenamento no ambiente local via composição de contêineres fornecida pelo starter-kit; o gerador de eventos de teste do starter-kit publica mensagens sem chave de partição, o que reforça a impossibilidade de assumir ordenação por conta.
