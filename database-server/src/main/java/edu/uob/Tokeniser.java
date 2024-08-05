package edu.uob;

import java.util.ArrayList;
import java.util.Arrays;


public class Tokeniser {

    String query;
    String[] specialCharacters = {"(", ")", ",", ";", "<", "<=", ">=", ">", "==", "!=", "="};

    ArrayList<String> tokens = new ArrayList<String>();


    public void setup() {
        query = query.trim();
        String[] fragments = query.split("'");
        for (int i = 0; i < fragments.length; i++) {
            if (i % 2 != 0) tokens.add("'" + fragments[i] + "'");
            else {
                String[] nextBatchOfTokens = tokenise(fragments[i]);
                tokens.addAll(Arrays.asList(nextBatchOfTokens));
            }
        }
   }


   public String[] tokenise(String input) {
       for (int i = 0; i < specialCharacters.length; i++) {
           input = input.replace(specialCharacters[i], " " + specialCharacters[i] + " ");
       }
       while (input.contains("  ")) input = input.replaceAll("  ", " ");
       input = input.trim();
       return input.split(" ");

   }

    public ArrayList<String> connectStringLiterals(ArrayList<String> initialTokens) {
        ArrayList<String> consolidatedTokens = new ArrayList<>();
        StringBuilder literalBuilder = new StringBuilder();
        boolean inLiteral = false;
        for (String token : initialTokens) {
            if (inLiteral) {
                literalBuilder.append(" ").append(token);
                if (token.endsWith("'")) {
                    consolidatedTokens.add(literalBuilder.toString());
                    literalBuilder.setLength(0); // Reset the builder
                    inLiteral = false;
                }
            } else {
                if (token.startsWith("'") && !token.endsWith("'")) {
                    // start of a string literal
                    literalBuilder.append(token);
                    inLiteral = true;
                } else {
                    consolidatedTokens.add(token); // it isnt a string literal so append it
                }
            }
        }
        // If there's an incomplete string literal at the end, add what's been built so far
        if (literalBuilder.length() > 0) {
            consolidatedTokens.add(literalBuilder.toString());
        }
        return consolidatedTokens;
    }


    public ArrayList<String> combineEqualityTokens(ArrayList<String> tokens) {
        ArrayList<String> combinedTokens = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            String currentToken = tokens.get(i);
            if (currentToken.equals("=") && (i + 1 < tokens.size()) && tokens.get(i + 1).equals("=")) {
                combinedTokens.add("==");
                i++; // skip the next "="
            }
            else if (currentToken.equals(">") && (i + 1 < tokens.size()) && tokens.get(i + 1).equals("=")) {
                combinedTokens.add(">=");
                i++;
            }
            else if (currentToken.equals("<") && (i + 1 < tokens.size()) && tokens.get(i + 1).equals("=")) {
                combinedTokens.add("<=");
                i++;
            }
            else if (currentToken.equals("!") && (i + 1 < tokens.size()) && tokens.get(i + 1).equals("=")) {
                combinedTokens.add("!=");
                i++;
            }
            else {
                // For all other tokens, add them to the array as normal
                combinedTokens.add(currentToken);
            }
        }
        return combinedTokens;
    }



//    public ArrayList<String> combineEqualityTokens(ArrayList<String> tokens) {
//        ArrayList<String> combinedTokens = new ArrayList<>();
//        for (int i = 0; i < tokens.size(); i++) {
//            String currentToken = tokens.get(i);
//            if (currentToken.equals("=") && (i + 1 < tokens.size()) && tokens.get(i + 1).equals("=")) {
//                combinedTokens.add("==");
//                i++; // skip the second '='
//            } else {
//                combinedTokens.add(currentToken);
//            }
//        }
//        return combinedTokens;
//    }

}


