# ADR-0005: Representação de dinheiro e de tempo

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípio IV; `specs/001-consulta-saldo/research.md` R-06; `data-model.md` seções 2, 4.2 e 5

## Contexto

O serviço lê saldos e instantes de eventos JSON, decide precedência por tempo, grava no DynamoDB e devolve o saldo por HTTP. Saldo
errado é pior que indisponibilidade (Princípio IV): qualquer passagem por `Double`, arredondamento silencioso ou perda de
precisão de timestamp é defeito cumulativo e invisível. O DynamoDB `N` aceita no máximo 38 dígitos significativos e normaliza
zeros à direita; JSON Number não carrega escala (RFC 8259); os timestamps da origem estão em microssegundos.

## Decisão

**Dinheiro**

- `BigDecimal` ponta a ponta (desserialização, domínio, persistência e resposta); `Double`/`Float` são proibidos. O domínio expõe
  `Money(amount: BigDecimal, currency: CurrencyCode)`, com `CurrencyCode` restrito a códigos ISO 4217 conhecidos por
  `java.util.Currency`, em maiúsculas.
- No DynamoDB, `balanceAmount` é `N`, escrito com `BigDecimal.toPlainString()` (sem notação científica) e lido com
  `BigDecimal(String)`.
- **Normalização de zeros não é perda**: o DynamoDB devolve `183.1` para `183.10`; o valor é idêntico, a origem também envia JSON
  Number (a escala recebida é artefato do serializador) e a Constitution IV a exclui expressamente de "perda de valor". Por isso
  `Money` compara por valor numérico (`compareTo == 0`, `hashCode` sobre `stripTrailingZeros()`).
- **Apresentação**: a escala é completada até as casas padrão da moeda somente na resposta, sem nunca arredondar
  (`Money.paddedToCurrencyScale()`: BRL `183.1` vira `183.10`, `10.123` permanece `10.123`, JPY `500` permanece `500`, moedas
  com `-1` casas, como XAU, ficam inalteradas). A serialização usa notação plana.
- **Teto de 38 dígitos**: precisão de até 38 dígitos significativos (medida por `BigDecimal.precision()` do valor recebido;
  zeros à direita contam, conservador) e escala positiva de até 38. Escala negativa (`1E+3`) é expandida para escala zero só se
  `precisão - escala <= 38`, de modo que `1E999999999` é rejeitado sem materializar o expoente. Violação: `invalid_value`,
  sempre antes do banco (o DynamoDB rejeitaria 39 dígitos com erro 400).
- `Money` não expõe o valor em `toString`, para que saldos não vazem em logs por acidente.

**Tempo**

- Instantes em microssegundos como `Long` (`EventInstant`), sem passar por `Double` nem por `Instant` até a hora de expor; a
  conversão para `Instant` usa `Math.floorDiv/floorMod`, obrigatório para valores negativos.
- O mínimo depende do papel do campo: `transaction.timestamp >= 2000-01-01T00:00:00Z` (detecta segundos e milissegundos enviados
  por engano e entra na precedência); `account.created_at >= 1900-01-01T00:00:00Z` (contas anteriores a 2000 são legítimas,
  anteriores a 1970 têm microssegundos negativos, e o campo não entra na precedência). O máximo (`agora + tolerância`) fica na
  camada `application`, que dispõe do `Clock`.
- `updated_at` na resposta é ISO 8601 com offset, no fuso de exibição `America/Sao_Paulo` (configurável); o instante em UTC é a
  verdade e o offset é apresentação. O formato é o `DateTimeFormatter.ISO_OFFSET_DATE_TIME`: a fração de segundo **não tem zeros à
  direita** (`.433`, `.433123`, `.43`) e some quando é zero. É válido em ISO 8601 e reproduz o exemplo do enunciado (`...13.433-03:00`);
  um parser que exija exatamente 3 ou 6 dígitos precisa tolerar a fração variável. Largura fixa seria mudança de contrato e não foi
  adotada (evolução, se um consumidor exigir).
- Na **leitura**, os instantes persistidos são reidratados por `EventInstant.fromPersisted`, sem checagem de faixa: os mínimos
  (`BALANCE_MIN_*`) valem para o evento que entra, e uma configuração diferente da vigente na gravação não pode transformar um
  snapshot válido em erro 500.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| `balanceAmount` como `S` (texto plano) | Preserva escala que o contrato não garante; dinheiro "stringly typed", não numérico no banco |
| `N` em unidade mínima (centavos) | Exige conversão nos dois sentidos, tabela de casas por moeda e rejeitar ou arredondar frações de centavo |
| `Double`/`Float` | Erro de ponto flutuante: proibido pela Constitution IV |
| Arredondar para as casas da moeda | Perda silenciosa de valor: proibido; a escala só é completada, nunca reduzida |
| Timestamps em `Instant`/milissegundos no domínio | Perda de precisão de microssegundos ou custo de conversão sem ganho; `Long` é exato e ordenável |
| Um único mínimo para os dois campos de tempo | Rejeitaria contas legítimas anteriores a 2000 (DLT indevido) ou aceitaria timestamps em unidade errada na precedência |

## Consequências

- (+) Nenhuma conversão com perda de valor entre a entrada JSON e a resposta; regras de precisão e escala testadas no domínio,
  sem infraestrutura.
- (+) Dado inválido nunca alcança o banco: o limite de 38 dígitos é imposto antes de qualquer escrita.
- (-) Valores com mais de 38 dígitos significativos, ainda que numericamente pequenos por causa de zeros à direita, são
  rejeitados (decisão conservadora e documentada em teste).
- (-) A escala original enviada pela origem não é preservada (`183.10` volta como `183.1` do banco e é completada só na resposta).
