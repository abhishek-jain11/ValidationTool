
package com.dbvalidator.vo;

/**
 *
 * @author AJ
 * @since LMS b1308
 */
public class HanaTuple extends Tuple {

	@Override
	public String toString() {
		String msg = "\n Target Record ";
		msg += super.toString();
		return msg;
	}

}
