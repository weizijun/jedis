package redis.clients.jedis;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;

/**
 * @author hzweizijun
 * @date 2016年3月29日 下午3:09:01
 * 
 */
public class JedisClusterPipeline extends PipelineBase {
  private JedisCluster jedisCluster;
  private List<FutureResult> results = new ArrayList<FutureResult>();
  private Queue<Client> clients = new LinkedList<Client>();

  private static class FutureResult {
    private Client client;

    public FutureResult(Client client) {
      this.client = client;
    }

    public Object get() {
      return client.getOne();
    }
  }
  
  public void setJedisCluster(JedisCluster jedisCluster) {
    this.jedisCluster = jedisCluster;
  }

  public List<Object> getResults() {
    List<Object> r = new ArrayList<Object>();
    for (FutureResult fr : results) {
      r.add(fr.get());
    }
    return r;
  }
  
  public void sync() {
    for (Client client : clients) {
      Object result = null;
      try {
        result = client.getOne();
      } catch (JedisRedirectionException e) {
        e.printStackTrace();
      }
      
      generateResponse(result);
    }
  }
  
  public List<Object> syncAndReturnAll() {
    List<Object> formatted = new ArrayList<Object>();
    for (Client client : clients) {
      Object result = null;
      try {
        result = client.getOne();
      } catch (JedisRedirectionException e) {
        e.printStackTrace();
      }
      
      formatted.add(generateResponse(result).get());
    }
    return formatted;
  }  

  @Override
  protected Client getClient(String key) {
    Jedis jedis = jedisCluster.getConnectionHandler().getConnectionFromSlot(JedisClusterCRC16.getSlot(key));
    Client client = jedis.getClient();
    clients.add(client);
    results.add(new FutureResult(client));
    return client;
  }

  @Override
  protected Client getClient(byte[] key) {
    Jedis jedis = jedisCluster.getConnectionHandler().getConnectionFromSlot(JedisClusterCRC16.getSlot(key));
    Client client = jedis.getClient();
    clients.add(client);
    results.add(new FutureResult(client));
    return client;
  }

}
