package se.redfield.node.port.orientdb.util;

import java.util.concurrent.Future;

public class FutureUtil {
	public static <V> boolean isFinish(Future<V> future) {
		return future.isCancelled() || future.isDone();		
	}

}
