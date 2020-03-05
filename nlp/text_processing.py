from nlp.constants import terminology
import spacy
from spacy.tokens import Doc, Span
from spacy.matcher import Matcher
from spacy.symbols import NOUN, VERB
from itertools import takewhile

SUBJ_list = ['agent', 'csubj', 'csubjpass', 'expl', 'nsubj', 'nsubjpass']
OBJ_list = ['attr', 'dobj', 'dative', 'oprd', 'pobj']
AUX_list = ['aux', 'auxpass', 'neg']
prepositional_phrase = ['NOUN', 'ADP', 'PROPN']

class TextProcessor():

    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        print(self.nlp.pipe_names)
        my_matcher = MyMatcher(self.nlp, terminology, label='about')
        self.nlp.add_pipe(my_matcher, name="term_matcher", after='ner')

    def get_main_verbs_of_sent(self, sent):
        """Return the main (non-auxiliary) verbs in a sentence."""
        return [tok for tok in sent
                if tok.pos == VERB and tok.dep_ not in {'aux', 'auxpass'}]

    def get_subjects_of_verb(self, verb):
        """Return all subjects of a verb according to the dependency parse."""
        subjs = [tok for tok in verb.lefts
                 if tok.dep_ in SUBJ_list]
        # get additional conjunct subjects
        subjs.extend(tok for subj in subjs for tok in self._get_conjuncts(subj))
        return subjs

    def get_objects_of_verb(self, verb):
        """
        Return all objects of a verb according to the dependency parse,
        including open clausal complements.
        """
        objs = [tok for tok in verb.rights
                if tok.dep_ in OBJ_list]
        # get open clausal complements (xcomp)
        objs.extend(tok for tok in verb.rights
                    if tok.dep_ == 'xcomp')
        # get additional conjunct objects
        objs.extend(tok for obj in objs for tok in self._get_conjuncts(obj))
        return objs

    def get_preprositional_phrase(self, objs):
        """
        Receive the object and check if the object have predical phrase
        Here I want to extract patterns like: (NOUN + ADP + PROPN) or
        (NOUN + ADP + NOUN)
        """
        return [right for right in objs.rights
                if right.dep_ == 'prep']

    def get_span_for_subtree(self, obj):
        min_i = obj.i
        max_i = obj.i + sum(1 for _ in [right.subtree for right in obj.rights])
        return (min_i, max_i)

    def get_span_for_compound_noun(self, noun):
        """
        Return document indexes spanning all (adjacent) tokens
        in a compound noun.
        """
        min_i = noun.i - sum(1 for _ in takewhile(lambda x: x.dep_ == 'compound',
                                                  reversed(list(noun.lefts))))
        return (min_i, noun.i)

    def get_span_for_verb_auxiliaries(self, verb):
        """
        Return document indexes spanning all (adjacent) tokens
        around a verb that are auxiliary verbs or negations.
        """
        min_i = verb.i - sum(1 for _ in takewhile(lambda x: x.dep_ in AUX_list,
                                                  reversed(list(verb.lefts))))
        max_i = verb.i + sum(1 for _ in takewhile(lambda x: x.dep_ in AUX_list,
                                                  verb.rights))
        return (min_i, max_i)

    def _get_conjuncts(self, tok):
        """
        Return conjunct dependents of the leftmost conjunct in a coordinated phrase,
        e.g. "Burton, [Dan], and [Josh] ...".
        """
        return [right for right in tok.rights
                if right.dep_ == 'conj']

    def subject_verb_object(self, sent, start_i):
        """
        Extract an ordered sequence of subject-verb-object (SVO) triples from a
        spacy-parsed doc. Note that this only works for SVO languages.

        Args:
            doc (``textacy.Doc`` or ``spacy.Doc`` or ``spacy.Span``)

        Yields:
            Tuple[``spacy.Span``, ``spacy.Span``, ``spacy.Span``]: the next 3-tuple
                of spans from ``doc`` representing a (subject, verb, object) triple,
                in order of appearance
        """

        obj_pp = []
        verbs = self.get_main_verbs_of_sent(sent)
        for verb in verbs:
            subjs = self.get_subjects_of_verb(verb)
            if not subjs:
                continue
            objs = self.get_objects_of_verb(verb)
            if not objs:
                continue

            for subj in subjs:
                subj = sent[self.get_span_for_compound_noun(subj)[0] - start_i: subj.i - start_i + 1]
                for obj in objs:

                    if obj.pos == NOUN:
                        span = self.get_span_for_compound_noun(obj)
                        obj_pp = self.get_preprositional_phrase(obj)

                        if obj_pp:
                            span = (span[0], self.get_span_for_subtree(obj_pp[0])[1])

                    elif obj.pos == VERB:
                        span = self.get_span_for_verb_auxiliaries(obj)
                        obj_pp = self.get_preprositional_phrase(obj)
                        if obj_pp:
                            span = (span[0], self.get_span_for_subtree(obj_pp[0])[1])

                    else:
                        span = (obj.i, obj.i)
                        obj_pp = self.get_preprositional_phrase(obj)
                        if obj_pp:
                            span = (span[0], self.get_span_for_subtree(obj_pp[0])[1])
                    obj = sent[span[0] - start_i: span[1] - start_i + 1]
                    yield (subj, verb, obj)

    def print_subject_verb_object(self, doc):

        result = []

        if doc._.rule_match and len(doc._.rule_match) > 0:
            for match in doc._.rule_match:
                span = doc[match.start: match.end]  # matched span
                sent = span.sent

                start_i = sent[0].i
                for info in self.subject_verb_object(sent, start_i):

                    result.append({"topic": span.text, "subject": info[0].text, 'verb': info[1].text,
                                   'object': info[2].text})

                    print("Topic:\t{}\nSuject:\t{}\nVerb:\t{}\nObjet:\t{}".format(
                        span.text,
                        info[0].text,
                        info[1].text,
                        info[2].text))

        return result


class MyMatcher():

    def __init__(self, nlp, terminology, label='Match', function=None):  # el constructor de nuestro Matcher

        self.matcher = Matcher(nlp.vocab)  # creamos un objeto Matcher
        for topic, patterns in terminology.items():  # cargamos los términos de la mini ontología
            for term in patterns:
                self.matcher.add(topic, function, term)
        Doc.set_extension('rule_match', default=False, force=True)  # creamos una extensión al objeto Doc para poder
        # almacenar esta información

    def __call__(self, doc):  # la función que se llamará desde el pipeline de spaCy
        matches = self.matcher(doc)  # aplicaremos el matcher sobre el Doc

        spans = []
        for label, start, end in matches:  # para cada termino encontrado
            span = Span(doc, start, end, label=label)  # crea un objeto Span
            spans.append(span)

        doc._.rule_match = spans  # guardamos los Spans en el atributo que habíamos declarado en el constructor
        return doc

if __name__ == '__main__':


    text = '''Uber pays $148 mn over data breach in latest image-boosting move. 
    SAN FRANCISCO.
    Uber agreed Wednesday to pay a $148 million penalty over a massive
    2016 data breach which the company concealed for a year, in the latest effort by the 
    global ridesharing giant to improve its image and move past its missteps from its early years.'''

    text_processor = TextProcessor()

    doc = text_processor.nlp(text)

    for sent in doc.sents:
        for token in sent:
            if not token.is_space:
                print("{:<15}{:<15}{}".format(
                    token.text,  # la palabra tal y como apareció en el texto
                    token.lemma_,  # su forma lematizada
                    token.pos_  # la categoría gramatical de la palabra
                ))
        print('\n')  # cada línea en blanco marca el final de una frase
    #=========================================================================

    text_processor.print_subject_verb_object(doc)