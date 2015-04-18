package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by cK on 3/30/15.
 */

public class Test {
    public static void main(String[] args) {
        StringBuilder stringBuilder = new StringBuilder(
                "6:5560:u67IfOtrJqQUalnHFpig2V7FojQW4XU2:dku6wHHXzJ2AwPBpv1l2XDp6nPrJslVf:JKbK9w2kh1atqY04NwCeEJUm0Wt1AeVW:4SljeQgMttvS7C0s5ui89FoMhPSr9tlO:L2B4EE6jIQ7nrx10AefLnt3WYOfNo3FC:GKpSC3va2IohlqzEwWiwO69UkcN8rwiM:xxa4Yng6Lq2f9zZx0MAatsaiooOE7vvc:o5OuTVGpibqvz2oid8Chcls5qrTsThhQ:57HJJgG8CASoL2ZOD5uS22u34z3hN1T5:xQZPmECGGcYoo1fuhmDuIh571BurUhky:");

        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        System.out.println(stringBuilder.toString());
        String[] data = stringBuilder.toString().split(":");

        for (int i = 0; i < data.length; i++) {
            System.out.println(data[i]);
        }
    }
}