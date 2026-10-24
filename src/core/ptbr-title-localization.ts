import { isPersistableAliasScript, isPortugueseAlias } from '../alias-sources/identity-verifier.js';
import { alternativeTitleKey, cleanAlternativeTitle, mergeAlternativeTitles } from './alternative-titles.js';

const CJK_REGEX = /[\u3040-\u30ff\u3400-\u9fff\uf900-\ufaff\uac00-\ud7af]/u;

// Scripts/diacritics exclusive to Vietnamese, Turkish, Polish, Czech, German, Pinyin, Italian/French grave accents, etc.
const NON_PT_LATIN_DIACRITICS_REGEX =
  /[\u1ea0-\u1ef9đĐığşİĞŞąćęłńśźżĄĆĘŁŃŚŹŻřůěščžťďňŘŮĚŠČŽŤĎŇäöüßÄÖÜèìòùÈÌÒÙāēīōūĀĒĪŌŪǎěǐǒǔǍĚǏǑǓǖǘǚǜǕǗǙǛ]/u;

const SPANISH_OR_FRENCH_EXCLUSIVE_REGEX =
  /[ñ¿¡]|^la\s+|(?<![\p{L}\p{N}\-])(?:el|los|las|del|una|unos|unas|hacia|también|después|pero|joven|jóvenes|corazón|dragón|prisión|traición|reencarnación|maldición|canción|pasión|habitación|educación|tentación|obsesión|redención|salvación|seguridad|cámaras|camaras|benjamín|jardín|capitán|común|según|algún|ningún|en\s+el|en\s+la|de\s+la|con\s+el|con\s+la|a\s+solas|mi\s+hermana|mi\s+esposa|por\s+fin|fin\s+contigo|solo\s+para|la\s+novia|buena\s+elección|elección|amas|soy\s+el|mamá|chica|chicas|llegada|vacaciones|verano|empapadas|recuerdo|perros|fuego|lucha|cuando\s+el|cuando\s+la|guía\s+del|venta|virgen|telefónico|viviendo\s+con|personalidades|cómo\s+ser|villanos|correctamente|relatos|actúe|jefecito|devorador|hambre|adopta|nella|della|degli|delle|nello|ragazza|dans|avec|pour|une|château|leçons|garçon|cœur|sœur|pas\s+de|d'os|de\s+bonne\s+foi)(?![\p{L}\p{N}])/iu;

const STRONG_ENGLISH_MARKERS =
  /(?<![\p{L}\p{N}])(?:the|and|with|from|into|without|within|between|through|after|before|under|over|about|against|during|because|although|though|while|where|when|what|which|who|whom|whose|why|how|this|that|these|those|my|your|his|her|its|our|their|mine|yours|hers|ours|theirs|myself|yourself|himself|herself|itself|ourselves|themselves|i'm|i'll|i've|i'd|you're|you'll|you've|he's|she's|it's|we're|they're|don't|doesn't|didn't|won't|wouldn't|can't|couldn't|shouldn't|isn't|aren't|wasn't|weren't|haven't|hasn't|hadn't|let's|there's|here's|what's|who's|that's|girl|boy|woman|man|women|men|wife|husband|mother|father|sister|brother|daughter|son|aunt|auntie|uncle|niece|nephew|cousin|stepmother|stepmom|stepsister|stepbrother|stepdaughter|girlfriend|boyfriend|fiance|fiancee|fiancée|lover|maid|teacher|student|nurse|doctor|boss|secretary|neighbor|neighbour|housewife|widow|virgin|slut|bitch|whore|knight|mage|wizard|witch|hero|villain|villainess|duke|duchess|prince|princess|king|queen|emperor|empress|saint|saintess|demon|devil|angel|dragon|monster|tower|academy|school|highschool|college|guild|world|life|death|love|heart|soul|body|mind|blood|night|day|time|year|years|summer|winter|spring|autumn|fall|morning|evening|secret|story|tale|chronicles|chronicle|legend|return|reincarnation|reincarnated|rebirth|reborn|regression|regressed|regressor|leveling|awakening|awakened|strongest|weakest|greatest|first|second|third|last|only|alone|another|other|new|old|young|little|big|great|good|bad|evil|dark|black|white|red|blue|golden|silver|sweet|bitter|hot|cold|warm|cool|wet|dry|pure|dirty|lewd|naughty|wild|crazy|mad|insane|married|divorced|cheating|betrayal|revenge|vengeance|contract|marriage|wedding|divorce|pregnant|pregnancy|training|lesson|lessons|service|services|playing|house|home|room|apartment|mansion|castle|palace|island|forest|city|town|village|country|empire|kingdom)(?![\p{L}\p{N}])/iu;

const STRONG_ROMAJI_MARKERS =
  /(?<![\p{L}\p{N}])(?:wo|wa|ga|desu|masu|mashita|masen|ashita|kyou|kinou|watashi|boku|ore|atashi|kimi|anata|kare|kanojo|karera|tachi|oniisan|otouto|imouto|okaasan|otousan|ojisan|obasan|ojiisan|obaasan|musume|musuko|tsuma|otto|kareshi|tomodachi|osananajimi|hitozuma|jukujo|joshi|joshikousei|chichi|oppai|oshiri|manko|chinko|chinpo|nakadashi|paizuri|ferachio|onani|onanii|sekkusu|hamemakuri|netorare|netori|saimin|chikan|kankin|choukyou|ryoujoku|mesu|neko|usagi|kitsune|ookami|ryuu|maou|yuusha|tensei|shoukan|mahou|ouja|oukoku|gakuen|gakkou|bukatsu|ensei|gasshuku|onsen|ryokan|byouin|shokuin|kaisha|shachou|buchou|senmu|kateikyoushi|meido|shitsuji|hime|ouji|oujo|kishi|majo|megami|tenshi|akuma|youkai|yuurei|bakemono|mononoke|shinigami|kamisama|kami|kokoro|karada|inochi|koi|yume|namida|egao|kisu|seikatsu|nichijou|monogatari|nikki|kiroku|himitsu|yakusoku|unmei|kiseki|fukushuu|kekkon|rikon|ninshin|shussan|shojo|shoujo|shounen|seinen|josei|zenpen|kouhen|bangaihen|gekijou|dakishimete|shimatta|shite|sarete|dekiru|dekinai|korosu|koroshite|shinu|ikasete|kimochii)(?![\p{L}\p{N}])/iu;

const PT_WORDS = new Set([
  'o', 'a', 'os', 'as', 'um', 'uma', 'uns', 'umas', 'do', 'da', 'dos', 'das',
  'no', 'na', 'nos', 'nas', 'ao', 'aos', 'à', 'às', 'pelo', 'pela', 'pelos', 'pelas',
  'de', 'em', 'por', 'para', 'com', 'sem', 'sob', 'sobre', 'entre', 'até', 'após', 'contra', 'desde', 'perante',
  'se', 'ou', 'nem', 'mas', 'pois', 'então', 'assim', 'lá', 'cá', 'aqui', 'ali', 'aí', 'além', 'atrás', 'frente', 'lado', 'dentro', 'fora', 'cima', 'baixo', 'meio', 'fim', 'início', 'começo', 'parte', 'partes', 'capítulo', 'volume', 'cena', 'episódio', 'especial',
  'meu', 'minha', 'meus', 'minhas', 'seu', 'sua', 'seus', 'suas', 'nosso', 'nossa', 'nossos', 'nossas',
  'eu', 'tu', 'ele', 'ela', 'nós', 'eles', 'elas', 'você', 'vocês', 'mim', 'comigo', 'contigo', 'consigo',
  'este', 'esta', 'estes', 'estas', 'esse', 'essa', 'esses', 'essas', 'aquele', 'aquela', 'isto', 'isso', 'aquilo',
  'que', 'quem', 'qual', 'quais', 'onde', 'quando', 'como', 'porque', 'porquê', 'quanto',
  'não', 'sim', 'mais', 'menos', 'muito', 'muita', 'muitos', 'muitas', 'pouco', 'pouca', 'todo', 'toda', 'todos', 'todas',
  'tudo', 'nada', 'algo', 'alguém', 'ninguém', 'outro', 'outra', 'outros', 'outras', 'mesmo', 'mesma', 'mesmos', 'mesmas',
  'já', 'ainda', 'sempre', 'nunca', 'jamais', 'também', 'só', 'apenas', 'quase', 'bem', 'mal', 'hoje', 'ontem', 'amanhã', 'agora', 'depois', 'antes', 'cedo', 'tarde',
  'é', 'são', 'foi', 'foram', 'era', 'eram', 'ser', 'estar', 'está', 'estão', 'estava', 'estavam', 'fui', 'vai', 'vão', 'ir',
  'tem', 'têm', 'tinha', 'tinham', 'ter', 'havia', 'houve', 'pode', 'podem', 'posso', 'quer', 'quero', 'querem', 'queria',
  'faz', 'fazer', 'fez', 'feito', 'feita', 'disse', 'diz', 'dizer', 'sei', 'sabe', 'saber', 'vi', 'viu', 'ver', 'deu', 'dar',
  'fica', 'ficar', 'ficou', 'vem', 'veio', 'vir', 'vou', 'vamos', 'sou', 'somos', 'estou', 'estamos',
  'dois', 'duas', 'três', 'quatro', 'cinco', 'seis', 'sete', 'oito', 'nove', 'dez', 'cem', 'mil', 'milhão',
  'amor', 'vida', 'morte', 'mundo', 'tempo', 'dia', 'dias', 'noite', 'noites', 'hora', 'horas', 'ano', 'anos', 'vez', 'vezes',
  'homem', 'homens', 'mulher', 'mulheres', 'garoto', 'garota', 'garotos', 'garotas', 'menino', 'menina', 'meninos', 'meninas', 'rapaz', 'moça',
  'pai', 'mãe', 'pais', 'filho', 'filha', 'filhos', 'filhas', 'irmão', 'irmã', 'irmãos', 'irmãs', 'irmãzinha', 'irmãozinho', 'família',
  'tio', 'tia', 'titia', 'titio', 'primo', 'prima', 'avô', 'avó', 'vovó', 'vovô', 'neto', 'neta', 'sobrinho', 'sobrinha', 'madrasta', 'madrastas', 'padrasto', 'enteada', 'enteadas', 'enteado', 'sogra', 'sogro', 'cunhada', 'cunhado', 'nora', 'genro', 'gêmea', 'gêmeas', 'gêmeo', 'gêmeos',
  'marido', 'maridos', 'esposa', 'esposas', 'noivo', 'noiva', 'namorado', 'namorados', 'namorada', 'namoradas', 'amante', 'amantes', 'casal', 'casamento', 'divórcio', 'viúva', 'viúvas', 'viúvo',
  'amigo', 'amiga', 'amigos', 'amigas', 'colega', 'colegas', 'vizinho', 'vizinha', 'vizinhos', 'vizinhas', 'chefe', 'patrão', 'patroa', 'secretária', 'secretárias', 'empregada', 'empregadas', 'criada', 'mordomo',
  'professor', 'professora', 'professores', 'professoras', 'aluno', 'aluna', 'alunos', 'alunas', 'estudante', 'estudantes', 'veterano', 'veterana', 'calouro', 'caloura', 'colegial', 'colegiais', 'universitária', 'universitárias', 'universitário',
  'médico', 'médica', 'médicos', 'médicas', 'doutor', 'doutora', 'enfermeira', 'enfermeiras', 'enfermeiro', 'paciente', 'policial', 'detetive', 'juiz', 'advogado', 'advogada', 'gerente', 'diretor', 'diretora', 'presidente', 'treinador', 'treinadora', 'cliente', 'clientes', 'hóspede', 'inquilino', 'inquilina', 'senhorio', 'senhoria',
  'rei', 'rainha', 'príncipe', 'princesa', 'duque', 'duquesa', 'marquês', 'marquesa', 'conde', 'condessa', 'barão', 'baronesa', 'lorde', 'dama', 'imperador', 'imperatriz', 'império', 'reino', 'corte', 'palácio', 'castelo', 'trono',
  'vossa', 'alteza', 'majestade', 'graça', 'excelência', 'senhor', 'senhora', 'senhorita', 'sr', 'sra', 'srta', 'dr', 'dra', 'dona', 'donas', 'dono', 'donos', 'dom', 'são', 'santa', 'santo', 'olá',
  'herói', 'heróis', 'heroína', 'vilão', 'vilã', 'vilões', 'vilãs', 'monstro', 'monstros', 'demônio', 'demônios', 'demoníaco', 'demoníaca', 'diabo', 'anjo', 'anjos', 'deus', 'deusa', 'deuses', 'divino', 'divina', 'sagrado', 'sagrada', 'maldito', 'maldita', 'maldição', 'bênção',
  'dragão', 'dragões', 'fada', 'fadas', 'elfo', 'elfa', 'elfos', 'elfas', 'orc', 'orcs', 'goblin', 'goblins', 'vampiro', 'vampira', 'lobisomem', 'fantasma', 'fantasmas', 'espírito', 'espíritos', 'zumbi', 'bruxa', 'bruxo', 'feiticeiro', 'feiticeira', 'mago', 'maga', 'magia', 'magias', 'mágico', 'mágica', 'necromante', 'curandeiro', 'curandeira', 'alquimista', 'súcubo', 'sereia', 'tentáculo', 'tentáculos', 'androide', 'robô', 'boneca', 'boneco',
  'guerreiro', 'guerreira', 'cavaleiro', 'cavaleira', 'espadachim', 'espada', 'espadas', 'lâmina', 'escudo', 'arco', 'flecha', 'lança', 'adaga', 'arma', 'armas', 'armadura', 'bárbaro', 'bárbara', 'paladino', 'sacerdote', 'sacerdotisa', 'freira', 'freiras', 'monge', 'culto', 'seita', 'cultivo', 'cultivador', 'marcial', 'marciais',
  'caçador', 'caçadora', 'caçadores', 'assassino', 'assassina', 'mercenário', 'mercenária', 'ladrão', 'ladra', 'pirata', 'piratas', 'soldado', 'capitão', 'capitã', 'comandante', 'líder', 'mestre', 'mestra', 'discípulo', 'discípula', 'aprendiz', 'assistente', 'veterinário', 'veterinária', 'escravo', 'escrava', 'escravos', 'escravas', 'servo', 'serva', 'prisioneiro', 'prisioneira', 'prisão',
  'aventureiro', 'aventureira', 'guilda', 'masmorra', 'calabouço', 'torre', 'labirinto', 'nível', 'níveis', 'habilidade', 'habilidades', 'sistema', 'classe', 'poder', 'poderes', 'força',
  'reencarnação', 'reencarnado', 'reencarnada', 'reencarnei', 'renascido', 'renascida', 'renascimento', 'retorno', 'retornou', 'regressão', 'regressor', 'regressei', 'transmigração', 'apocalipse', 'despertar', 'evolução',
  'academia', 'escola', 'colégio', 'universidade', 'clube', 'aula', 'aulas', 'sala', 'quarto', 'casa', 'mansão', 'apartamento', 'dormitório', 'pousada', 'hotel', 'hospital', 'escritório', 'empresa', 'loja', 'café', 'restaurante', 'praia', 'ilha', 'floresta', 'montanha', 'cidade', 'vila', 'aldeia', 'campo', 'fazenda', 'jardim', 'inferno', 'paraíso', 'céu', 'terra', 'mar', 'oceano', 'rio', 'lago', 'banho', 'banheiro', 'chuveiro', 'piscina', 'cama', 'cozinha', 'trem', 'ônibus', 'metrô', 'acampamento', 'caverna', 'subterrâneo', 'subterrâneos', 'subterrânea', 'crepúsculo',
  'coração', 'alma', 'corpo', 'mente', 'sangue', 'lágrimas', 'sorriso', 'olhos', 'olhar', 'voz', 'mão', 'mãos', 'peito', 'seios', 'peitões', 'peitão', 'bunda', 'bundão', 'rabo', 'rabuda', 'lábios', 'beijo', 'beijos', 'abraço', 'toque', 'desejo', 'desejos', 'paixão', 'prazer', 'tentação', 'sedução', 'pecado', 'pecador', 'pecadora', 'obsessão', 'ciúme', 'ciúmes', 'inveja', 'ódio', 'raiva', 'medo', 'dor', 'culpa', 'vergonha', 'orgulho', 'esperança', 'sonho', 'sonhos', 'pesadelo', 'memória', 'memórias', 'lembrança', 'promessa', 'segredo', 'segredos', 'mentira', 'mentiras', 'verdade', 'destino', 'sorte', 'azar', 'milagre', 'vingança', 'justiça', 'paz', 'guerra', 'batalha', 'luta', 'vitória', 'derrota', 'fuga', 'caça', 'jogo', 'jogos', 'vôlei', 'futebol', 'regra', 'regras', 'contrato', 'pacto', 'acordo', 'aposta', 'dívida', 'dívidas', 'missão', 'tarefa', 'trabalho', 'emprego', 'férias', 'viagem', 'festa', 'festival', 'encontro', 'encontros', 'lição', 'treino', 'treinamento', 'exame', 'teste', 'prova', 'resgate', 'pagamento', 'recompensa', 'recompensas', 'castigo', 'punição', 'chantagem', 'refém', 'flagra', 'blecaute', 'maratona',
  'história', 'histórias', 'conto', 'contos', 'crônica', 'crônicas', 'lenda', 'lendas', 'diário', 'registro', 'registros', 'guia', 'manual', 'método', 'caminho', 'arte', 'lei', 'teoria', 'projeto', 'plano', 'ponto', 'vista', 'leitor', 'onisciente', 'repente',
  'bom', 'boa', 'bons', 'boas', 'mau', 'má', 'maus', 'más', 'grande', 'grandes', 'pequeno', 'pequena', 'pequenos', 'pequenas', 'gigante', 'gigantes', 'novo', 'nova', 'novos', 'novas', 'velho', 'velha', 'antigo', 'antiga', 'jovem', 'jovens', 'infância', 'adulto', 'adultos', 'adulta', 'adultas', 'madura', 'maduras', 'coroa', 'coroas',
  'belo', 'bela', 'lindo', 'linda', 'bonito', 'bonita', 'feio', 'feia', 'fofo', 'fofa', 'doce', 'doces', 'amargo', 'quente', 'frio', 'fria', 'claro', 'clara', 'escuro', 'escura', 'sombrio', 'sombria', 'negro', 'negra', 'branco', 'branca', 'vermelho', 'vermelha', 'escarlate', 'azul', 'verde', 'dourado', 'dourada', 'prateado', 'prateada',
  'forte', 'fraco', 'fraca', 'poderoso', 'poderosa', 'invencível', 'supremo', 'suprema', 'absoluto', 'absoluta', 'infinito', 'infinita', 'eterno', 'eterna', 'imortal', 'imortais', 'mortal', 'lendário', 'lendária', 'mítico', 'mítica', 'épico', 'épica', 'único', 'única', 'primeiro', 'primeira', 'último', 'última', 'segundo', 'segunda', 'terceiro', 'terceira', 'total', 'final', 'real', 'reais', 'absurdo', 'absurda', 'relutante', 'elástico', 'elástica',
  'rico', 'rica', 'pobre', 'nobre', 'imperial', 'imperiais', 'errante', 'besta', 'bestas', 'falso', 'falsa', 'verdadeiro', 'verdadeira', 'certo', 'certa', 'errado', 'errada', 'fácil', 'difícil', 'impossível', 'possível', 'perigoso', 'perigosa', 'seguro', 'segura', 'secreto', 'secreta', 'oculto', 'oculta', 'proibido', 'proibida', 'perdido', 'perdida', 'esquecido', 'esquecida', 'abandonado', 'abandonada', 'roubado', 'roubada', 'quebrado', 'quebrada', 'espelhado', 'espelhada', 'sujo', 'suja', 'limpo', 'limpa', 'molhado', 'molhada',
  'louco', 'louca', 'insano', 'insana', 'obsessivo', 'obsessiva', 'possessivo', 'possessiva', 'ciumento', 'ciumenta', 'cruel', 'gentil', 'tímido', 'tímida', 'quieto', 'quieta', 'solitário', 'solitária', 'inocente', 'puro', 'pura', 'virgem', 'virgindade', 'safado', 'safada', 'safadas', 'pervertido', 'pervertida', 'tarado', 'tarada', 'devasso', 'devassa', 'sacana', 'sacanas', 'sacanagem', 'infiel', 'fiel', 'leal', 'traidor', 'traidora', 'traído', 'traída', 'traição', 'corno', 'cornudo', 'vadia', 'vadias', 'puta', 'putas', 'putaria', 'prostituta', 'bêbado', 'bêbada', 'casado', 'casada', 'casadas', 'casados', 'solteiro', 'solteira', 'solteiras', 'gordinha', 'bobinha', 'peituda', 'peitudas', 'bunduda', 'bundudas', 'gostosa', 'gostosas', 'deliciosa', 'sensual', 'erótico', 'erótica', 'sexual', 'sexo', 'pornô', 'porno', 'porra', 'gozo', 'sêmen', 'leite', 'esperma', 'afrodisíaco', 'afrodisíacos', 'hipnose', 'hipnotizada', 'hipnotizado', 'submissa', 'submisso', 'incesto', 'harém', 'orgia', 'suruba', 'boquete', 'anal', 'tesão', 'pau', 'rola', 'piroca', 'buceta', 'xoxota', 'cu', 'roupa', 'roupas', 'calcinha', 'calcinhas', 'biquíni', 'saia', 'vestido', 'uniforme', 'depósito', 'visita', 'domiciliar', 'maneira', 'lua', 'mel',
  'feliz', 'triste', 'sozinho', 'sozinha', 'junto', 'juntos', 'juntas', 'livre', 'preso', 'presa', 'vivo', 'viva', 'morto', 'morta', 'grávida', 'grávidas', 'gravidez',
  'futuro', 'passado', 'presente', 'logando', 'subindo', 'caindo', 'vivendo', 'morrendo', 'amando', 'odiando', 'fugindo', 'lutando', 'buscando', 'procurando', 'esperando', 'sonhando', 'brincando', 'jogando', 'treinando', 'estudando', 'trabalhando', 'comendo', 'bebendo', 'dormindo', 'acordando', 'sorrindo', 'chorando', 'fodendo', 'fodê', 'transando', 'chupando', 'gozando', 'sentando', 'dando', 'pegando', 'roubando', 'matando', 'salvando', 'protegendo', 'criando', 'destruindo', 'conquistando', 'dominando', 'domando', 'cuidando', 'ensinando', 'aprendendo', 'ajudando', 'usando', 'fazendo', 'tendo', 'sendo', 'violentando', 'abusando', 'seduzindo', 'provocando', 'traindo', 'lavando', 'lava', 'paga', 'pagar', 'deixa', 'deixam', 'deixar', 'imagens', 'pés', 'pé', 'compêndio', 'milênio', 'vá', 'embora', 'grátis', 'véu', 'torna', 'tornou', 'tornei', 'virei', 'virou', 'virando',
]);

// Internationally consecrated franchise titles adopted as-is in Brazil
export const CONSECRATED_PROPER_TITLES = new Set([
  'one piece',
  'bleach',
  'naruto',
  'boruto',
  'dragon ball',
  'dragon ball z',
  'dragon ball super',
  'dragon ball gt',
  'jujutsu kaisen',
  'chainsaw man',
  'solo leveling',
  'solo leveling: ragnarok',
  'lookism',
  'blue lock',
  'dandadan',
  'spy x family',
  'sakamoto days',
  'kagurabachi',
  'berserk',
  'vagabond',
  'monster',
  'hunter x hunter',
  'black clover',
  'fairy tail',
  'one punch man',
  'one-punch man',
  'mob psycho 100',
  'wind breaker',
  'tower of god',
  'the beginning after the end',
  'overgeared',
  'eleceed',
  'nano machine',
  'questism',
  'viral hit',
  'reality quest',
  'baki',
  'gantz',
  'claymore',
  'death note',
  'neon genesis evangelion',
  'evangelion',
  'akira',
  'inuyasha',
  'yu-gi-oh!',
  'yu-gi-oh',
  'pokémon',
  'pokemon',
  'digimon',
  'saint seiya',
  'hellsing',
  'fullmetal alchemist',
  'soul eater',
  'fire force',
  'dr. stone',
  'haikyuu!!',
  'haikyu!!',
  'slam dunk',
  'kingdom',
  'vinland saga',
  'tokyo ghoul',
  'tokyo ghoul:re',
  'tokyo revengers',
  'demon slayer',
  'kimetsu no yaiba',
  'attack on titan',
  'shingeki no kyojin',
  'my hero academia',
  'boku no hero academia',
  'oshi no ko',
  'frieren',
  'undead unluck',
  'mashle',
  'kaiju no. 8',
  'kaiju nº 8',
  'gachiakuta',
  'choujin x',
  'versus',
  'record of ragnarok',
  'shuumatsu no valkyrie',
  'goblin slayer',
  'overlord',
  're:zero',
  'konosuba',
  'danmachi',
  'sword art online',
  'fate/stay night',
  'fate/grand order',
  'tsukihime',
  'genshin impact',
  'honkai: star rail',
  'blue archive',
  'azur lane',
  'arknights',
  'nikke',
  'zelda',
  'resident evil',
  'final fantasy',
  'street fighter',
  'tekken',
  'king of fighters',
  'persona 5',
  'nier:automata',
]);

/**
 * Owner-specified editorial title preferences with confirmed work identity.
 * Keyed by canonical work UUID or canonical slug.
 */
export const OWNER_EDITORIAL_TITLE_PREFERENCES = new Map<string, string>([
  ['e1b4bce1-5fbd-4fdf-8930-ca4afc26771d', 'Um Pedaço'],
  ['one-piece-ptbr', 'Um Pedaço'],
]);

/**
 * Titles specifically rejected from promotion because they belong to European Portuguese (PT-PT)
 * or non-Brazilian translations.
 */
export const PT_PT_REJECTED_ALIASES = new Set<string>([
  'bola de dragao',
  'bola de dragão',
  'ataque dos gigantes',
]);

export function cleanPtBrCandidateTitle(raw: string): string | null {
  if (!raw || typeof raw !== 'string') return null;
  const stripped = raw
    .replace(/\s*\((?:Brazil|Brasil|PT-BR|PT|Português)\)\s*$/i, '')
    .replace(/\s*-\s*Ler\s+(?:Mang[aá]|Manhwa|Manhua|Comics|Hentai)\s+Online.*$/i, '')
    .trim();
  return cleanAlternativeTitle(stripped);
}

function isPtBrClause(clause: string): boolean {
  const title = clause.trim();
  if (!title) return false;
  if (
    !isPersistableAliasScript(title) ||
    CJK_REGEX.test(title) ||
    NON_PT_LATIN_DIACRITICS_REGEX.test(title) ||
    SPANISH_OR_FRENCH_EXCLUSIVE_REGEX.test(title)
  ) {
    return false;
  }
  if (isPortugueseAlias(title)) {
    return true;
  }

  const hasStrongEn = STRONG_ENGLISH_MARKERS.test(title);
  const hasStrongRomaji = STRONG_ROMAJI_MARKERS.test(title);

  // Genuine PT nasal/cedilla/circumflex markers (excluding Turkish/French which were filtered above)
  if (/[ãõ]|ç[ãõaou]|(?<![\p{L}\p{N}])(?:pornô|vôlei|sêmen|fodê|você|vocês|avô|metrô|robô)(?![\p{L}\p{N}])/iu.test(title)) {
    return true;
  }

  const words = title
    .toLowerCase()
    .replace(/[^\p{L}\s]/gu, ' ')
    .split(/\s+/)
    .filter(Boolean);
  if (words.length === 0) return false;

  const isMorphologicalPt = (w: string) =>
    w.length >= 5 &&
    /(?:ção|ções|nho|nha|nhos|nhas|lho|lha|lhos|lhas|mente|ando|endo|indo|ada|ado|adas|ados|ida|ido|idas|idos|ista|istas|ismo|ismos|eiro|eira|eiros|eiras|ador|adora|adores|adoras|oso|osa|osos|osas|ável|ível|áveis|íveis|ência|ências|ância|âncias|amento|amentos|imento|imentos)$/i.test(w) &&
    !hasStrongEn &&
    !hasStrongRomaji;

  const ROMAJI_AMBIGUOUS_PT_WORDS = new Set(['a', 'o', 'e', 'de', 'da', 'do', 'na', 'no', 'se', 'me', 'ano', 'sono']);

  const nonTrivialPtHits = words.filter(
    (w) =>
      (w === 'é' || w === 'à' || (w.length >= 2 && w !== 'no' && w !== 'me' && w !== 'de')) &&
      !(hasStrongRomaji && ROMAJI_AMBIGUOUS_PT_WORDS.has(w)) &&
      (PT_WORDS.has(w) || isMorphologicalPt(w)),
  ).length;

  const ptHits = words.filter((w) => {
    if ((w === 'a' || w === 'o' || w === 'e' || w === 'de' || w === 'in' || w === 'me' || w === 'no') && (hasStrongEn || hasStrongRomaji) && nonTrivialPtHits < 2) {
      return false;
    }
    if (hasStrongRomaji && ROMAJI_AMBIGUOUS_PT_WORDS.has(w) && nonTrivialPtHits < 2) {
      return false;
    }
    if (PT_WORDS.has(w) || isMorphologicalPt(w)) return true;
    return false;
  }).length;

  if (hasStrongEn || hasStrongRomaji) {
    return nonTrivialPtHits >= 2 && ptHits / words.length >= 0.5;
  }

  if (/[áéíóúâêôà]/i.test(title) && ptHits >= 1) return true;
  if (words.length === 1) return PT_WORDS.has(words[0]) && words[0].length >= 4;
  if (words.length === 2) return ptHits === 2 || (nonTrivialPtHits >= 1 && /[áéíóúâêôà]/i.test(title));
  return ptHits >= 2 && ptHits / words.length >= 0.4;
}

export function isPtBrTitle(rawTitle: string): boolean {
  const title = cleanPtBrCandidateTitle(rawTitle);
  if (!title) return false;
  if (
    !isPersistableAliasScript(title) ||
    CJK_REGEX.test(title) ||
    NON_PT_LATIN_DIACRITICS_REGEX.test(title) ||
    SPANISH_OR_FRENCH_EXCLUSIVE_REGEX.test(title)
  ) {
    return false;
  }
  if (/[{}<>]|-->|Média\s+\d|getElementById|Ler\s+Comics\s+Online/i.test(title)) {
    return false;
  }

  // Strip trailing parenthetical franchise tag like "Plain Ol' Wife (Laboratório de Dexter)"
  const withoutParen = title.replace(/\s*\([^)]+\)\s*$/, '').trim();
  const baseToCheck = withoutParen.length >= 3 ? withoutParen : title;

  if (isPtBrClause(baseToCheck)) {
    return true;
  }

  // Check clauses separated by colon, en-dash, em-dash, or spaced hyphen (e.g. "Dragon Ball Z Hentai – As Férias da Chichi")
  const clauses = baseToCheck
    .split(/\s*(?:[:–—~]|\s-\s)\s*/)
    .map((c) => c.trim())
    .filter((c) => c.length >= 4);
  if (clauses.length >= 2) {
    return clauses.some((c) => isPtBrClause(c));
  }

  return false;
}

export function isConsecratedProperTitle(rawTitle: string): boolean {
  const key = alternativeTitleKey(rawTitle);
  if (!key) return false;
  return CONSECRATED_PROPER_TITLES.has(key);
}

export interface AutomaticPtBrTitlePromotionInput {
  workId?: string;
  slug?: string;
  title: string;
  aliases: readonly string[];
  provenance?: Record<string, any> | null;
}

export interface AutomaticPtBrTitlePromotionResult {
  promoted: boolean;
  newTitle: string;
  newAliases: string[];
  method?: 'OWNER_EDITORIAL_PREFERENCE' | 'PTBR_PROMOTED_EXISTING_ALIAS';
  ownerEditorialPreference?: boolean;
}

/**
 * 4-TIER LOCALIZATION POLICY:
 * 1. Explicit Owner Editorial Preference (e.g. One Piece -> Um Pedaço).
 * 2. Protected PT-BR Title (manual, ptbr_primary_titles_v1, or validated PT-BR) -> immutable anti-reversion.
 * 3. Verified PT-BR Alias Promotion (promote clean PT-BR alias; franchise fame does not block promotion).
 * 4. Preserved Consecrated / Proper Name / International Title when no PT-BR alias exists.
 */
export function resolveAutomaticPtBrTitlePromotion(
  input: AutomaticPtBrTitlePromotionInput,
): AutomaticPtBrTitlePromotionResult {
  const currentTitle = (input.title || '').trim();
  const currentAliases = Array.isArray(input.aliases) ? [...input.aliases] : [];
  const provTitle = input.provenance?.title;

  // 1. TIER 1 — EXPLICIT OWNER EDITORIAL PREFERENCE
  const ownerPreferredTitle =
    (input.workId && OWNER_EDITORIAL_TITLE_PREFERENCES.get(input.workId)) ||
    (input.slug && OWNER_EDITORIAL_TITLE_PREFERENCES.get(input.slug)) ||
    (provTitle?.owner_editorial_preference === true ? currentTitle : null);

  if (ownerPreferredTitle) {
    if (alternativeTitleKey(currentTitle) === alternativeTitleKey(ownerPreferredTitle)) {
      // Already set to the owner preferred title. Ensure zero oscillation.
      return {
        promoted: false,
        newTitle: currentTitle,
        newAliases: currentAliases,
        ownerEditorialPreference: true,
      };
    }
    // Promote to the owner's explicitly chosen PT-BR title and preserve current title in aliases.
    const mergedAliases = mergeAlternativeTitles(
      currentAliases.filter((a) => alternativeTitleKey(cleanPtBrCandidateTitle(a) || a) !== alternativeTitleKey(ownerPreferredTitle)),
      [currentTitle],
      ownerPreferredTitle,
    );
    return {
      promoted: true,
      newTitle: ownerPreferredTitle,
      newAliases: mergedAliases,
      method: 'OWNER_EDITORIAL_PREFERENCE',
      ownerEditorialPreference: true,
    };
  }

  // 2. TIER 2 — PROTECTED PT-BR PRIMARY TITLE (ANTI-REVERSION & ANTI-PING-PONG)
  if (
    provTitle?.source === 'manual' ||
    provTitle?.localized_ptbr === true ||
    provTitle?.source === 'ptbr_primary_titles_v1' ||
    (provTitle as any)?.proper_name_preserved === true ||
    isPtBrTitle(currentTitle)
  ) {
    return {
      promoted: false,
      newTitle: currentTitle,
      newAliases: currentAliases,
    };
  }

  // 3. TIER 3 — VERIFIED PT-BR ALIAS PROMOTION
  for (const rawAlias of currentAliases) {
    const cleaned = cleanPtBrCandidateTitle(rawAlias);
    if (!cleaned || cleaned.length < 3 || cleaned.length > 200) continue;
    const aliasKey = alternativeTitleKey(cleaned);
    if (
      isPtBrTitle(cleaned) &&
      !PT_PT_REJECTED_ALIASES.has(aliasKey) &&
      !isConsecratedProperTitle(cleaned)
    ) {
      const mergedAliases = mergeAlternativeTitles(
        currentAliases.filter((a) => alternativeTitleKey(cleanPtBrCandidateTitle(a) || a) !== aliasKey),
        [currentTitle],
        cleaned,
      );
      return {
        promoted: true,
        newTitle: cleaned,
        newAliases: mergedAliases,
        method: 'PTBR_PROMOTED_EXISTING_ALIAS',
      };
    }
  }

  // 4. TIER 4 — CONSECRATED / PROPER NAME / INTERNATIONAL PRESERVATION
  return {
    promoted: false,
    newTitle: currentTitle,
    newAliases: currentAliases,
  };
}
