package se.redfield.node.port.orientdb.function;

import java.util.LinkedList;
import java.util.List;

class Utils {
	static String extractName(String funcNameWithParameters) {
		String name = null;
		if (funcNameWithParameters.indexOf("[") > -1) {
			name = funcNameWithParameters.substring(0, funcNameWithParameters.indexOf("["));
		} else {
			name = funcNameWithParameters;
		}
		return name;
	}

	static List<String> extractParameterNames(String funcNameWithParameters) {
		List<String> parameters = new LinkedList<>();
		if (funcNameWithParameters.indexOf("[") > -1) {
			String parametersStr = funcNameWithParameters.substring(funcNameWithParameters.indexOf("[") + 1,
					funcNameWithParameters.lastIndexOf("]"));
			for (String param : parametersStr.split(",")) {
				if (!param.trim().isEmpty()) {
					parameters.add(param.trim());
				}
			}
		}
		return parameters;
	}
}
