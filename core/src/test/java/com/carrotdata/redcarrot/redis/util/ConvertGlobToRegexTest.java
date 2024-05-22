package com.carrotdata.redcarrot.redis.util;
import static com.carrotdata.redcarrot.redis.util.Utils.convertGlobToRegex;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;

import org.junit.Ignore;
import org.junit.Test;

public class ConvertGlobToRegexTest {

    @Test
    public void testNonPrintableChars() {
      String s1 = "aaa\00";
      byte[] b = s1.getBytes(Charset.forName("us-ascii"));
      System.out.println();
    }
    
    @Test
    public void testRedisGlob() {
      String[] redis = new String[] {
        "h?llo", "h*llo","h**llo", "h[ae]llo", "h[^e]llo","h[a-c]llo", "?[a-cA-C]*",
        "h[^e]llo", "h[^a-c]llo", "[^e]llo", "h*", "ha*", "[o]", "[^o]", "*", "?", "*?", "?*"
      };
      String[] input = new String[] {"hello", "hallo", "hbllo", "hdllo", "hublo", "a" , "o", "oo", "hcllo", "cat", "bat", "Cat"};
      for (int i = 0; i < redis.length; i++) {
        String javaRegex = convertGlobToRegex(redis[i]);
        System.out.println();
        for (int j = 0; j < input.length; j++) {
          System.out.printf("redis=%s java=%s input=%s matches=%b\n", redis[i], javaRegex, input[j], input[j].matches(javaRegex));
        }
        System.out.println();
      }
      @SuppressWarnings("unused")
      String s1 = convertGlobToRegex("h[^^^]llo");
      
    }
    @Test
    public void testSimpleStarConversion() {
        assertEquals("^.*$", convertGlobToRegex("*"));
    }

    @Test
    public void testSimpleQuestionConversion() {
        assertEquals("^.$", convertGlobToRegex("?"));
    }

    @Test
    public void testMixedPatternConversion() {
        assertEquals("^abc.*def$", convertGlobToRegex("abc*def"));
    }

    @Test
    public void testMultipleStarsAndQuestions() {
        assertEquals("^a.*b.*c.d$", convertGlobToRegex("a*b*c?d"));
    }

    @Test
    public void testCharacterClass() {
        assertEquals("^a[bc]d$", convertGlobToRegex("a[bc]d"));
    }

    @Test
    public void testNegatedCharacterClass() {
        assertEquals("^a[^bc]d$", convertGlobToRegex("a[^bc]d"));
    }

    @Ignore
    @Test
    public void testEscapedSpecialCharacters() {
      String exp = "^a\\.\\*\\?\\[\\]\\(\\)\\{\\}\\+\\|\\^\\$$";
      String result = convertGlobToRegex("a.*?[](){}+|^$");
      System.out.println(exp);
      System.out.println(result);
      assertEquals(exp, result);
    }

    @Test
    public void testEscapedGlobCharacters() {
        assertEquals("^a\\*\\?b$", convertGlobToRegex("a\\*\\?b"));
    }
    @Test
    public void testEmptyString() {
        assertTrue("".matches(convertGlobToRegex("")));
    }

    @Test
    public void testLiteralMatch() {
        assertTrue("abc".matches(convertGlobToRegex("abc")));
    }

    @Test
    public void testSingleStarMatch() {
        assertTrue("abcd".matches(convertGlobToRegex("*")));
    }

    @Test
    public void testStarAtEnd() {
        assertTrue("hello".matches(convertGlobToRegex("hel*")));
    }

    @Test
    public void testStarAtBeginning() {
        assertTrue("hello".matches(convertGlobToRegex("*lo")));
    }

    @Test
    public void testStarInTheMiddle() {
        assertTrue("abcde".matches(convertGlobToRegex("a*e")));
    }

    @Test
    public void testMultipleStars() {
        assertTrue("ababab".matches(convertGlobToRegex("*a*b*")));
    }

    @Test
    public void testQuestionMark() {
        assertTrue("a".matches(convertGlobToRegex("?")));
    }

    @Test
    public void testMultipleQuestionMarks() {
        assertTrue("abc".matches(convertGlobToRegex("???")));
    }

    @Test
    public void testCombinationOfStarAndQuestionMark() {
        assertTrue("abcde".matches(convertGlobToRegex("a*?de")));
    }

    @Test
    public void testComplexPattern() {
        assertTrue("axyz123".matches(convertGlobToRegex("a*?23")));
    }

    @Test
    public void testSpecialCharacters() {
        assertTrue("a[bc]d".matches(convertGlobToRegex("a\\[bc\\]d")));
    }

    @Test
    public void testPatternNotMatching() {
        assertTrue(!"abc".matches(convertGlobToRegex("a*d")));
    }

    @Test
    public void testMatchWithEscapedGlobCharacters() {
        assertTrue("a*b".matches(convertGlobToRegex("a\\*b")));
    }
    
    @Test
    public void star_becomes_dot_star() throws Exception {
        assertEquals("^gl.*b$", convertGlobToRegex("gl*b"));
    }

    @Test
    public void escaped_star_is_unchanged() throws Exception {
        assertEquals("^gl\\*b$", convertGlobToRegex("gl\\*b"));
    }

    @Test
    public void question_mark_becomes_dot() throws Exception {
        assertEquals("^gl.b$", convertGlobToRegex("gl?b"));
    }

    @Test
    public void escaped_question_mark_is_unchanged() throws Exception {
        assertEquals("^gl\\?b$", convertGlobToRegex("gl\\?b"));
    }

    @Test
    public void character_classes_dont_need_conversion() throws Exception {
        assertEquals("^gl[-o]b$", convertGlobToRegex("gl[-o]b"));
    }

    @Test
    public void escaped_classes_are_unchanged() throws Exception {
        assertEquals("^gl\\[-o\\]b$", convertGlobToRegex("gl\\[-o\\]b"));
    }

    @Test
    public void negation_in_character_classes() throws Exception {
        assertEquals("^gl[^a-n!p-z]b$", convertGlobToRegex("gl[!a-n!p-z]b"));
    }

    @Test
    public void nested_negation_in_character_classes() throws Exception {
        assertEquals("^gl[[^a-n]!p-z]b$", convertGlobToRegex("gl[[!a-n]!p-z]b"));
    }

    @Test
    public void donot_escape_carat_if_it_is_the_first_char_in_a_character_class() throws Exception {
        assertEquals("^gl[^o]b$", convertGlobToRegex("gl[^o]b"));
    }

    @Test
    public void metachars_are_escaped() throws Exception {
        assertEquals("^gl..*\\.\\(\\)\\+\\|\\^\\$\\@\\%b$", convertGlobToRegex("gl?*.()+|^$@%b"));
    }

    @Ignore
    @Test
    public void metachars_in_character_classes_dont_need_escaping() throws Exception {
        assertEquals("^gl[?*.()+|^$@%]b$", convertGlobToRegex("gl[?*.()+|^$@%]b"));
    }

    @Test
    public void escaped_backslash_is_unchanged() throws Exception {
        assertEquals("^gl\\\\b$", convertGlobToRegex("gl\\\\b"));
    }

    @Test
    public void slashQ_and_slashE_are_escaped() throws Exception {
        assertEquals("^\\\\Qglob\\\\E$", convertGlobToRegex("\\Qglob\\E"));
    }

    @Test
    public void braces_are_turned_into_groups() throws Exception {
        assertEquals("^(glob|regex)$", convertGlobToRegex("{glob,regex}"));
    }

    @Test
    public void escaped_braces_are_unchanged() throws Exception {
        assertEquals("^\\{glob\\}$", convertGlobToRegex("\\{glob\\}"));
    }

    @Test
    public void commas_dont_need_escaping() throws Exception {
        assertEquals("^(glob,regex),$", convertGlobToRegex("{glob\\,regex},"));
    }
}

