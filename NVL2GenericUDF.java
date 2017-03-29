package com.venkat.hiveGeneticUDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class NVL2GenericUDF extends GenericUDF {

	private ObjectInspector[] argumentOIs;
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		if (arguments.length < 3) {
			throw new UDFArgumentLengthException(
					"The function nvl2(string1, value_if_not_null, value_if_null) needs " 
					+ "at least three arguments.");
		}

		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but "
						+ arguments[i].getTypeName() + " is passed.");
			}
		}

		argumentOIs = arguments;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
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
		if (arguments[0].get() == null) {
			// return value_if_null
			returnValue = returnOIResolver.convertIfNecessary(arguments[2].get(), argumentOIs[2]);
		} else {
			returnValue = returnOIResolver.convertIfNecessary(arguments[1].get(), argumentOIs[1]);
		}

		return returnValue;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("nvl2 (");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}
	
	
}
