package com.atguigu;

import java.io.IOException;
import java.util.List;

import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;



public class SecKill_redis {

	private static final  org.slf4j.Logger logger =LoggerFactory.getLogger(SecKill_redis.class) ;

	/*
	 * 1.秒杀逻辑：
	 * 		 参数uid: 用户id
	 * 		产品id:  prodid
	 * 
	 * 		redis中存储两组数据：
	 * 				秒杀成功的用户：  key:  success_user  value: set(保证一个用户只能成功秒杀一次)
	 * 				记录产品的剩余库存：  key:  product  value: string 
	 * 
	 * 		实现：   ① 判断uid是否已经秒杀成功过，如果已经成功，拒绝再次秒杀
	 * 					boolean exsits=jedis.sismember(success_user,uid);
	 *			②用户首次秒杀：
	 *					a) 获取产品库存： string store= jedis.get(product)
	 *						null:  商家没有上架商品，未初始化商品
	 *						<=0:  库存没有了，秒杀失败，响应false
	 *						>0:   进入秒杀流程
	 *							将用户加入到秒杀成功的名单：  jedis.sadd(success_user,uid);
	 *							库存自减少1：  jedis.decr(product)
	 *
	 *2. 压力测试工具：  ab(apache bench),linux自带
	 *		 ab -n 2000 -c 200 -p /root/postarg -T 'application/x-www-form-urlencoded'  http://192.168.0.182:8080/MySeckill/doseckill
	 *					-n:  请求总数
	 *					-c:  并发数
	 *					-p：  post请求传参
	 *					-T： 表单内容类型
	 *3. 超买问题！
	 *		解决： 使用乐观锁解决！
	 *				加锁：  watch(key)
	 *
	 *4. 秒杀不公平问题：  先抢的没抢到，后抢的抢到！
	 *			缺少秩序！ 抢购的顺序！
	 *			解决的办法：  将混乱的操作，变成有序的操作！
	 *				①单线程（不现实）
	 *				②将整个doSecKill中的所有操作，整体作为一个原子！
	 *						引入lua脚本！
	 *
	 */
	public static boolean doSecKill(String uid,String prodid) throws IOException {
		
		// 生成相关的key
		String productKey="sk:"+prodid+":product";
		String userKey="sk:"+prodid+":user";
		
		//通过连接池来获取jedis
		Jedis jedis = JedisPoolUtil.getJedisPoolInstance().getResource();
		//判断用户是否已经秒杀成功过
		if (jedis.sismember(userKey, uid)) {
			
			System.out.println(uid+"已经抢过！");
			
			jedis.close();
			
			return false;
		}
		
		// 加锁，来监视库存
		jedis.watch(productKey);
		
		String product_store = jedis.get(productKey);
		//判断商品是否上架
		if (product_store==null) {
			
			System.out.println("商家还未上架商品！");
				
			jedis.close();
			
			return false;
			
		}
		
		int store = Integer.parseInt(product_store);
		
		if (store<=0) {
			
			System.out.println("商品已经抢光！");
			
			jedis.close();
			
			return false;
			
		}
		
		//==================正式进入抢购流程==================
		
		//使用事务组队
		Transaction transaction = jedis.multi();
		
		transaction.sadd(userKey, uid);
		
		transaction.decr(productKey);
		
		//执行
		List<Object> result = transaction.exec();
		
		if (result.size()!=2) {
			
			System.out.println("抢购失败！");
			
			jedis.close();
			
			return false;
		}
		
		System.out.println(uid+"抢购成功！");
		
		jedis.close();
		
		return true;
		
	}
	

}
