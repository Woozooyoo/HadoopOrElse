package com.weibo;

import com.weibo.hbaseFunction.*;
import com.weibo.hbaseInitialize.HBaseUtil;

import java.io.IOException;
import java.util.List;

/**
 * 发布微博
 * 互粉
 * 取关
 * 查看微博
 * 1、内容content表 rowKey:发布人uid_ts   cf:info     c:content   value微博内容
 * 2、用户的关注和粉丝表 rowKey:uid   cf是attend和fan c:otherUid    value:otherUid
 * 3、用户的收件箱表 rowKey:我的uid     cf:info     c:关注的人的Uid  value:otherUid_ts   ts:ts
 * <p>
 * 互相关注维度 共同关注 可能认识的人
 *
 * @author Adrian
 */
public class WeiBo {
	/**
	 * 测试
	 *
	 * @param
	 * @throws IOException
	 */
	//发布微博
	public static void publishFunctionDaoTest(String uid, String content) throws IOException {
		PublishContent.publishContent (uid, content);
	}

	//关注
	public static void addAttendTest(String uid, String... attends) throws IOException {
		AddAttends.addAttends (uid, attends);
	}

	//取关
	public static void removeAttendTest(String uid, String... attends) throws IOException {
		RemoveAttends.removeAttends (uid, attends);
	}

	//刷微博
	public static void scanFunctionDaoContentTest(String uid) throws IOException {
		List<Message> list = GetAttendsContent.getAttendsContent (uid);
		System.out.println (list);
	}

	//看某人微博
	public static void scanSomebodyContentTest(String otherUid) throws IOException {
		List<Message> list = GetSomebodyContent.getSomebodyContent (otherUid);
		System.out.println (list);
	}

	public static void main(String[] args) throws IOException {
//		HBaseUtil.initTableMutualConcern ();
//		HBaseUtil.init ();
//
//		publishFunctionDaoTest( "1002", "哦，我的上帝，我要踢爆他的屁股");
//		publishFunctionDaoTest( "1002", "哦，我的上帝，我还要踢爆他的屁股");
//		publishFunctionDaoTest( "1002", "哦，我的上帝，我非要踢爆他的屁股");
//		publishFunctionDaoTest( "1003", "哦，我的上帝，我也要踢爆他的屁股");
//
//		addAttendTest ( "1002", "1001");
//		removeAttendTest ( "1001", "1002");
//		removeAttendTest ( "1001", "1002", "1003");
//		addAttendTest ( "1003", "1002", "1001");
		addAttendTest ( "1001",  "1003");

//		scanFunctionDaoContentTest ("1001");
//		scanFunctionDaoContentTest ("1002");
//		scanFunctionDaoContentTest ("1003");

//		publishFunctionDaoTest ( "1001", "嘿嘿嘿11");
//		publishFunctionDaoTest ( "1001", "嘿嘿嘿22");
//		publishFunctionDaoTest ( "1001", "嘿嘿嘿33");
//		publishFunctionDaoTest ( "1001", "嘿嘿嘿44");
//		publishFunctionDaoTest ( "1001", "嘿嘿嘿55");
//		publishFunctionDaoTest ( "1001", "嘿嘿嘿66");
//		scanFunctionDaoContentTest ( "1003");
//		scanSomebodyContentTest ( "1003");
	}
}