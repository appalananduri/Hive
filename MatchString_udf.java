package com.venkat.hiveGeneticUDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

public class MatchString extends GenericUDF {

	private ObjectInspector[] argumentOIs;
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
	private Text result = new Text();
	Pattern _pattern;
		
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		argumentOIs = arguments;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		_pattern = Pattern.compile(".*Message-Id:.(.*?)\\{CR\\}\\{LF\\}.*?");
		
		if (arguments.length > 2) {
			throw new UDFArgumentLengthException(
					"The function MatchString(string) needs one argument only ....");
			
		}

		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but "
						+ arguments[i].getTypeName() + " is passed.");
			}
		}

		
		
		for (int i = 0; i < arguments.length; i++) {
			if (!returnOIResolver.update(arguments[i])) {
				throw new UDFArgumentTypeException(i, "The value of return should have the same type: \""
						+ returnOIResolver.get().getTypeName() + "\" is expected but \"" 
						+ arguments[i].getTypeName()
						+ "\" is found");
			}
		}

		return returnOIResolver.get() ;
	}
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		
		 Object returnValue = null;
		 String MatchedString = null ;
		 
				
		if ( arguments[0].toString() == null || arguments[0].toString()== "" ) {
			result.set("");
			return result ; 
		}
		
		
		returnValue = returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);
		
		MatchedString = returnValue.toString() ;
		Matcher m = _pattern.matcher(MatchedString);
		
		if (m.find() && m.groupCount() == 1)
		{
			result.set(m.group(1)) ;
			return result ;
		}
		
		result.set("");
		return result ;

	}	
	
	
	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("MessageId");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}
	
}
