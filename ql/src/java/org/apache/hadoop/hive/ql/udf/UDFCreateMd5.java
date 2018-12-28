package org.apache.hadoop.hive.ql.udf;

import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;

/**
 * 通过字符串创建md5
 */
public class UDFCreateMd5 extends UDF {

	public  String evaluate(String ... column_datas) throws ParseException {
		if (column_datas == null){
			return "";
		}

		StringBuffer sb = new StringBuffer();
		for (String column_data:column_datas) {
			sb.append(column_data + "_");
		}

		String md5String = Hashing.md5().hashString(sb.toString().trim()).toString();

		return md5String;

 }
public static void main(String[] args) throws Exception {
		UDFCreateMd5 UDFCreateMd5 =new UDFCreateMd5();
		System.out.println(UDFCreateMd5.evaluate("2012-02-29","asdsad","asdsadwqe"));
	}
}
