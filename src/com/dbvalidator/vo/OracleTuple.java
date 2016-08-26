
package com.dbvalidator.vo;

/**
 *
 * @author AJ
 */
public class OracleTuple extends Tuple {

	@Override
	public String toString() {
		String msg = "\n Source Record ";
		msg += super.toString();
		return msg;
	}

}
