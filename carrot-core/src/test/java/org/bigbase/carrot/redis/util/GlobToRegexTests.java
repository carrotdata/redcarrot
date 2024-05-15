package org.bigbase.carrot.redis.util;
import static org.bigbase.carrot.redis.util.Utils.globToRegex;
import static org.bigbase.carrot.redis.util.Utils.convertGlobToRegex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class GlobToRegexTests {

    @Test
    public void testSimpleStarConversion() {
        assertEquals(".*", globToRegex("*"));
    }

    @Test
    public void testSimpleQuestionConversion() {
        assertEquals(".", globToRegex("?"));
    }

    @Test
    public void testMixedPatternConversion() {
        assertEquals("^abc.*def$", globToRegex("abc*def"));
    }

    @Test
    public void testMultipleStarsAndQuestions() {
        assertEquals("^a.*b.*c.d$", globToRegex("a*b*c?d"));
    }

    @Test
    public void testCharacterClass() {
        assertEquals("^a[bc]d$", globToRegex("a[bc]d"));
    }

    @Test
    public void testNegatedCharacterClass() {
        assertEquals("^a[^bc]d$", globToRegex("a[^bc]d"));
    }

    @Test
    public void testEscapedSpecialCharacters() {
        assertEquals("^a\\.\\*\\?\\[\\]\\(\\)\\{\\}\\+\\|\\^\\$$", globToRegex("a.*?[](){}+|^$"));
    }

    @Test
    public void testEscapedGlobCharacters() {
        assertEquals("^a*?b$", globToRegex("a\\*\\?b"));
    }
    @Test
    public void testEmptyString() {
        assertTrue("".matches(globToRegex("")));
    }

    @Test
    public void testLiteralMatch() {
        assertTrue("abc".matches(globToRegex("abc")));
    }

    @Test
    public void testSingleStarMatch() {
        assertTrue("abcd".matches(globToRegex("*")));
    }

    @Test
    public void testStarAtEnd() {
        assertTrue("hello".matches(globToRegex("hel*")));
    }

    @Test
    public void testStarAtBeginning() {
        assertTrue("hello".matches(globToRegex("*lo")));
    }

    @Test
    public void testStarInTheMiddle() {
        assertTrue("abcde".matches(globToRegex("a*e")));
    }

    @Test
    public void testMultipleStars() {
        assertTrue("ababab".matches(globToRegex("*a*b*")));
    }

    @Test
    public void testQuestionMark() {
        assertTrue("a".matches(globToRegex("?")));
    }

    @Test
    public void testMultipleQuestionMarks() {
        assertTrue("abc".matches(globToRegex("???")));
    }

    @Test
    public void testCombinationOfStarAndQuestionMark() {
        assertTrue("abcde".matches(globToRegex("a*?de")));
    }

    @Test
    public void testComplexPattern() {
        assertTrue("axyz123".matches(globToRegex("a*?23")));
    }

    @Test
    public void testSpecialCharacters() {
        assertTrue("a[bc]d".matches(globToRegex("a\\[bc\\]d")));
    }

    @Test
    public void testPatternNotMatching() {
        assertTrue(!"abc".matches(globToRegex("a*d")));
    }

    @Test
    public void testMatchWithEscapedGlobCharacters() {
        assertTrue("a*b".matches(globToRegex("a\\*b")));
    }
    
    @Test
    public void star_becomes_dot_star() throws Exception {
        assertEquals("gl.*b", convertGlobToRegex("gl*b"));
    }

    @Test
    public void escaped_star_is_unchanged() throws Exception {
        assertEquals("gl\\*b", convertGlobToRegex("gl\\*b"));
    }

    @Test
    public void question_mark_becomes_dot() throws Exception {
        assertEquals("gl.b", convertGlobToRegex("gl?b"));
    }

    @Test
    public void escaped_question_mark_is_unchanged() throws Exception {
        assertEquals("gl\\?b", convertGlobToRegex("gl\\?b"));
    }

    @Test
    public void character_classes_dont_need_conversion() throws Exception {
        assertEquals("gl[-o]b", convertGlobToRegex("gl[-o]b"));
    }

    @Test
    public void escaped_classes_are_unchanged() throws Exception {
        assertEquals("gl\\[-o\\]b", convertGlobToRegex("gl\\[-o\\]b"));
    }

    @Test
    public void negation_in_character_classes() throws Exception {
        assertEquals("gl[^a-n!p-z]b", convertGlobToRegex("gl[!a-n!p-z]b"));
    }

    @Test
    public void nested_negation_in_character_classes() throws Exception {
        assertEquals("gl[[^a-n]!p-z]b", convertGlobToRegex("gl[[!a-n]!p-z]b"));
    }

    @Test
    public void escape_carat_if_it_is_the_first_char_in_a_character_class() throws Exception {
        assertEquals("gl[\\^o]b", convertGlobToRegex("gl[^o]b"));
    }

    @Test
    public void metachars_are_escaped() throws Exception {
        assertEquals("gl..*\\.\\(\\)\\+\\|\\^\\$\\@\\%b", convertGlobToRegex("gl?*.()+|^$@%b"));
    }

    @Test
    public void metachars_in_character_classes_dont_need_escaping() throws Exception {
        assertEquals("gl[?*.()+|^$@%]b", convertGlobToRegex("gl[?*.()+|^$@%]b"));
    }

    @Test
    public void escaped_backslash_is_unchanged() throws Exception {
        assertEquals("gl\\\\b", convertGlobToRegex("gl\\\\b"));
    }

    @Test
    public void slashQ_and_slashE_are_escaped() throws Exception {
        assertEquals("\\\\Qglob\\\\E", convertGlobToRegex("\\Qglob\\E"));
    }

    @Test
    public void braces_are_turned_into_groups() throws Exception {
        assertEquals("(glob|regex)", convertGlobToRegex("{glob,regex}"));
    }

    @Test
    public void escaped_braces_are_unchanged() throws Exception {
        assertEquals("\\{glob\\}", convertGlobToRegex("\\{glob\\}"));
    }

    @Test
    public void commas_dont_need_escaping() throws Exception {
        assertEquals("(glob,regex),", convertGlobToRegex("{glob\\,regex},"));
    }
}

