package com.atguigu.youtube_syllabus.utils;

//SDNkMu8ZT68	w00dy911	630	People & Blogs	186	10181	3.49	494	257	rjnbgpPJUks	qlC39j5JImU

public class ETLUtil {
	public static String getETLString(String ori){
		
		StringBuilder sb = new StringBuilder();
		
		String[] splitsArray = ori.split("\t");
		
		if(splitsArray.length < 9) return null;
		
		splitsArray[3] = splitsArray[3].replaceAll(" ", "");

		for (int i = 0; i < splitsArray.length; i++) {
			
			sb.append(splitsArray[i]);

			if (i < 9) {
				if (i != splitsArray.length - 1) {
					sb.append("\t");
				}
			} else {
				if (i != splitsArray.length - 1) {
					sb.append("&");
				}
			}
		}
		return sb.toString();
	}
	
	/*public static void main(String[] args) {
		String ori ="SDNkMu8ZT68	w00dy911	630	People & Blogs	186	10181	3.49	494	257	";
		
		System.out.println(getETLString(ori));
	}*/
}