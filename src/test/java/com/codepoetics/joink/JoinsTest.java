package com.codepoetics.joink;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

public class JoinsTest {

    @Test
    public void
    joins_many_to_one() {
        List<Tuple2<Book, Author>> joined = Joins.join(Arrays.stream(books))
                .on(Book::getAuthorId)
                .to(Author::getId)
                .manyToOne(Arrays.stream(authors))
                .collect(Collectors.toList());


        assertThat(joined, hasItems(
                Tuple2.of(beingAndEvent, badiou),
                Tuple2.of(logicsOfWorlds, badiou),
                Tuple2.of(theoryOfTheSubject, badiou),
                Tuple2.of(antiOedipus, deleuzeAndGuattari),
                Tuple2.of(aThousandPlateaus, deleuzeAndGuattari),
                Tuple2.of(ofGrammatology, derrida),
                Tuple2.of(spectresOfMarx, derrida),
                Tuple2.of(totalityAndInfinity, levinas),
                Tuple2.of(cutOfTheReal, kolozova)
        ));
    }

    @Test public void
    joins_one_to_many() {
        List<Tuple2<Author, Set<Book>>> joined = Joins.join(Arrays.stream(authors))
                .on(Author::getId)
                .to(Book::getAuthorId)
                .oneToMany(Arrays.stream(books))
                .collect(Collectors.toList());

        assertThat(joined, hasItems(
                Tuple2.of(badiou, books(beingAndEvent, logicsOfWorlds, theoryOfTheSubject)),
                Tuple2.of(derrida, books(ofGrammatology, spectresOfMarx)),
                Tuple2.of(deleuzeAndGuattari, books(antiOedipus, aThousandPlateaus)),
                Tuple2.of(levinas, books(totalityAndInfinity)),
                Tuple2.of(kolozova, books(cutOfTheReal)),
                Tuple2.of(notAppearing, books())
        ));
    }

    @Test(expected=IllegalArgumentException.class) public void
    strict_one_to_one_join_fails_if_duplicate_left_elements() {
        List<Tuple2<Book, Author>> joined = Joins.join(Stream.of(beingAndEvent, logicsOfWorlds, ofGrammatology))
                .on(Book::getAuthorId)
                .to(Author::getId)
                .strictOneToOne(Stream.of(derrida, badiou))
                .collect(Collectors.toList());
    }

    @Test(expected=IllegalArgumentException.class) public void
    strict_one_to_many_join_fails_if_duplicate_left_elements() {
        List<Tuple2<Book, Set<Author>>> joined = Joins.join(Stream.of(beingAndEvent, logicsOfWorlds, ofGrammatology))
                .on(Book::getAuthorId)
                .to(Author::getId)
                .strictOneToMany(Stream.of(derrida, badiou))
                .collect(Collectors.toList());
    }

    @Test(expected=IllegalArgumentException.class) public void
    strict_one_to_many_join_fails_if_unmatched_right_elements() {
        List<Tuple2<Author, Set<Book>>> joined = Joins.join(Stream.of(derrida, kolozova, badiou))
                .on(Author::getId)
                .to(Book::getAuthorId)
                .strictOneToMany(Stream.of(beingAndEvent, totalityAndInfinity, ofGrammatology))
                .collect(Collectors.toList());
    }

    @Test() public void
    joins_one_to_many_strictly() {
        List<Tuple2<Author, Set<Book>>> joined = Joins.join(Stream.of(derrida, kolozova, badiou))
                .on(Author::getId)
                .to(Book::getAuthorId)
                .strictOneToMany(Stream.of(beingAndEvent, logicsOfWorlds, ofGrammatology, cutOfTheReal))
                .collect(Collectors.toList());

        assertThat(joined, hasItems(
                Tuple2.of(derrida, books(ofGrammatology)),
                Tuple2.of(badiou, books(beingAndEvent, logicsOfWorlds)),
                Tuple2.of(kolozova, books(cutOfTheReal))
        ));

    }

    @Test(expected=IllegalArgumentException.class) public void
    strict_one_to_one_join_fails_if_unmatched_left_elements() {
        List<Tuple2<Book, Author>> joined = Joins.join(Stream.of(beingAndEvent, totalityAndInfinity, ofGrammatology))
                .on(Book::getAuthorId)
                .to(Author::getId)
                .strictOneToOne(Stream.of(derrida, badiou))
                .collect(Collectors.toList());
    }

    @Test(expected=IllegalArgumentException.class) public void
    strict_one_to_one_join_fails_if_duplicate_right_elements() {
        List<Tuple2<Author, Book>> joined = Joins.join(Stream.of(derrida, badiou))
                .on(Author::getId)
                .to(Book::getAuthorId)
                .strictOneToOne(Stream.of(beingAndEvent, logicsOfWorlds, ofGrammatology))
                .collect(Collectors.toList());
    }

    @Test(expected=IllegalArgumentException.class) public void
    strict_one_to_one_join_fails_if_unmatched_right_elements() {
        List<Tuple2<Author, Book>> joined = Joins.join(Stream.of(derrida, badiou))
                .on(Author::getId)
                .to(Book::getAuthorId)
                .strictOneToOne(Stream.of(beingAndEvent, cutOfTheReal, ofGrammatology))
                .collect(Collectors.toList());
    }

    @Test() public void
    joins_one_to_one() {
        List<Tuple2<Book, Author>> joined = Joins.join(Stream.of(logicsOfWorlds, ofGrammatology))
                .on(Book::getAuthorId)
                .to(Author::getId)
                .strictOneToOne(Stream.of(derrida, badiou))
                .collect(Collectors.toList());

        assertThat(joined, hasItems(Tuple2.of(logicsOfWorlds, badiou), Tuple2.of(ofGrammatology, derrida)));
    }

    private static class Book {

        private final String id;
        private final String name;
        private final String authorId;

        private Book(String id, String name, String authorId) {
            this.id = id;
            this.name = name;
            this.authorId = authorId;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getAuthorId() {
            return authorId;
        }

        @Override public String toString() {
            return name;
        }
    }

    private static class Author {
        private final String id;
        private final String name;

        private Author(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        @Override public String toString() {
            return name;
        }
    }

    private static final Book beingAndEvent = new Book("BE", "Being And Event", "BAD");
    private static final Book totalityAndInfinity = new Book("TI", "Totality And Infinity", "LEV");
    private static final Book ofGrammatology = new Book("OG", "Of Grammatology", "DER");
    private static final Book logicsOfWorlds = new Book("LOW", "Logics Of Worlds", "BAD");
    private static final Book aThousandPlateaus = new Book("ATP", "A Thousand Plateaus", "DG");
    private static final Book antiOedipus = new Book("AO", "Anti-Oedipus", "DG");
    private static final Book spectresOfMarx = new Book("SM", "Spectres of Marx", "DER");
    private static final Book theoryOfTheSubject = new Book("TOS", "Theory of the Subject", "BAD");
    private static final Book cutOfTheReal = new Book("COR", "Cut Of The Real", "KK");
    private static final Book forgetFoucault = new Book("FF", "Forget Foucault", "BAU");

    private static final Book[] books = {
        beingAndEvent,
        totalityAndInfinity,
        ofGrammatology,
        logicsOfWorlds,
        aThousandPlateaus,
        spectresOfMarx,
        theoryOfTheSubject,
        antiOedipus,
        cutOfTheReal,
        forgetFoucault };

    private static final Author badiou = new Author("BAD", "Alain Badiou");
    private static final Author levinas = new Author("LEV", "Emmanuel Levinas");
    private static final Author deleuzeAndGuattari = new Author("DG", "Gilles Deleuze & Felix Guattari");
    private static final Author derrida = new Author("DER", "Jacques Derrida");
    private static final Author kolozova = new Author("KK", "Katerina Kolozova");
    private static final Author notAppearing = new Author("NA", "Sir Not-Appearing-In-This-Library");

    private static final Author[] authors = {
            badiou,
            levinas,
            deleuzeAndGuattari,
            derrida,
            kolozova,
            notAppearing
    };

    private Set<Book> books(Book...books) {
        return Arrays.stream(books).collect(Collectors.toSet());
    }
}
