package org.tron.program;

import ch.qos.logback.classic.Level;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StringUtils;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.overlay.client.DatabaseGrpcClient;
import org.tron.common.overlay.discover.DiscoverServer;
import org.tron.common.overlay.discover.node.NodeManager;
import org.tron.common.overlay.server.ChannelManager;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.Manager;
import org.tron.core.exception.AccountResourceInsufficientException;
import org.tron.core.exception.BadBlockException;
import org.tron.core.exception.BadItemException;
import org.tron.core.exception.BadNumberBlockException;
import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.exception.DupTransactionException;
import org.tron.core.exception.NonCommonBlockException;
import org.tron.core.exception.ReceiptCheckErrException;
import org.tron.core.exception.TaposException;
import org.tron.core.exception.TooBigTransactionException;
import org.tron.core.exception.TooBigTransactionResultException;
import org.tron.core.exception.TransactionExpirationException;
import org.tron.core.exception.UnLinkedBlockException;
import org.tron.core.exception.VMIllegalException;
import org.tron.core.exception.ValidateScheduleException;
import org.tron.core.exception.ValidateSignatureException;
import org.tron.core.services.RpcApiService;
import org.tron.core.services.http.solidity.SolidityNodeHttpApiService;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.DynamicProperties;

@Slf4j
public class SolidityNode {

  private DatabaseGrpcClient databaseGrpcClient;
  private Manager dbManager;

  private ScheduledExecutorService syncExecutor = Executors.newScheduledThreadPool(2);
  private ConcurrentHashMap<Long, BlockCapsule> blockChain = new ConcurrentHashMap<>();

  public void setDbManager(Manager dbManager) {
    this.dbManager = dbManager;
  }

  private void initGrpcClient(String addr) {
    try {
      databaseGrpcClient = new DatabaseGrpcClient(addr);
    } catch (Exception e) {
      logger.error("Failed to create database grpc client {}", addr);
      System.exit(0);
    }
  }

  private void shutdownGrpcClient() {
    if (databaseGrpcClient != null) {
      databaseGrpcClient.shutdown();
    }
  }

  private void syncLoop(Args args) {
//    while (true) {
//      try {
//        initGrpcClient(args.getTrustNodeAddr());
//        syncSolidityBlock();
//        shutdownGrpcClient();
//      } catch (Exception e) {
//        logger.error("Error in sync solidity block " + e.getMessage(), e);
//      }
//      try {
//        Thread.sleep(5000);
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//        e.printStackTrace();
//      }
//    }
  }

  private static AtomicLong lastSolidityBlockNum = new AtomicLong(0);

  private void syncSolidityBlock(String trustNodeAddr) {
    DynamicProperties remoteDynamicProperties = databaseGrpcClient.getDynamicProperties();
    long remoteLastSolidityBlockNum = remoteDynamicProperties.getLastSolidityBlockNum();

    ExecutorService syncPool = Executors.newFixedThreadPool(4);

    logger.info(
        "start get solidity block , lastSolidityBlockNum:{}, remoteLastSolidityBlockNum:{}",
        lastSolidityBlockNum, remoteLastSolidityBlockNum);
    while (true) {

      if (lastSolidityBlockNum.get() < remoteLastSolidityBlockNum && blockChain.size() < 100) {
        System.out.println(lastSolidityBlockNum.get() + "  " +
            remoteLastSolidityBlockNum + " " + blockChain.size());
        long getSolidityBlockNum = this.lastSolidityBlockNum.incrementAndGet();
        syncPool.submit(() -> {
          try {
            if (getSolidityBlockNum > remoteLastSolidityBlockNum) {
              return;
            }
            DatabaseGrpcClient grpcClient1 = new DatabaseGrpcClient(trustNodeAddr);
            Block block = grpcClient1.getBlock(getSolidityBlockNum);
            grpcClient1.shutdown();
            if (Objects.isNull(block)) {
              System.out.println(getSolidityBlockNum + "is null");
              return;
            }
            blockChain
                .put(block.getBlockHeader().getRawData().getNumber(), new BlockCapsule(block));

            if (0 == getSolidityBlockNum % 1000) {
              logger.info(
                  "get solidity block ,trx size {}, getSolidityBlockNum:{}, remoteLastSolidityBlockNum:{}",
                  block.getTransactionsCount(), getSolidityBlockNum, remoteLastSolidityBlockNum);
            }
          } catch (Exception e) {
            logger.error("Failed to create database grpc client {}", trustNodeAddr);
            System.exit(0);
          }
        });

      } else {
        break;
      }
    }
    syncPool.shutdown();
    logger.info("Sync with trust node completed!!!");
  }

  private void processSolidityChain() throws BadBlockException {
    while (true) {
      if (blockChain.size() <= 0) {
        break;
      }
      long lastSolidityBlockNum = dbManager.getDynamicPropertiesStore()
          .getLatestSolidifiedBlockNum();
      try {
        BlockCapsule blockCapsule = blockChain.get(lastSolidityBlockNum + 1);
        if (Objects.isNull(blockCapsule)) {
          logger.info("lastSolidityBlockNum:{} is   null, blockChainSize:{}",
              lastSolidityBlockNum + 1,
              blockChain.size());
          break;
        }
        logger.info(
            "process solidity block ,trx size {}, lastSolidityBlockNum:{}, remoteLastSolidityBlockNum:{}",
            blockCapsule.getInstance().getTransactionsCount(), lastSolidityBlockNum + 1,
            blockChain.size());
        dbManager.pushVerifiedBlock(blockCapsule);
        blockChain.remove(lastSolidityBlockNum + 1);
        for (TransactionCapsule trx : blockCapsule.getTransactions()) {
          TransactionInfoCapsule ret;
          try {
            ret = dbManager.getTransactionHistoryStore().get(trx.getTransactionId().getBytes());
          } catch (BadItemException ex) {
            logger.warn("", ex);
            continue;
          }
          ret.setBlockNumber(blockCapsule.getNum());
          ret.setBlockTimeStamp(blockCapsule.getTimeStamp());
          dbManager.getTransactionHistoryStore().put(trx.getTransactionId().getBytes(), ret);
        }
        dbManager.getDynamicPropertiesStore()
            .saveLatestSolidifiedBlockNum(lastSolidityBlockNum + 1);
      } catch (AccountResourceInsufficientException e) {
        throw new BadBlockException("validate AccountResource exception");
      } catch (ValidateScheduleException e) {
        throw new BadBlockException("validate schedule exception");
      } catch (ValidateSignatureException e) {
        throw new BadBlockException("validate signature exception");
      } catch (ContractValidateException e) {
        throw new BadBlockException("ContractValidate exception");
      } catch (ContractExeException e) {
        throw new BadBlockException("Contract Execute exception");
      } catch (TaposException e) {
        throw new BadBlockException("tapos exception");
      } catch (DupTransactionException e) {
        throw new BadBlockException("dup exception");
      } catch (TooBigTransactionException e) {
        throw new BadBlockException("too big exception");
      } catch (TooBigTransactionResultException e) {
        throw new BadBlockException("too big exception result");
      } catch (TransactionExpirationException e) {
        throw new BadBlockException("expiration exception");
      } catch (BadNumberBlockException e) {
        throw new BadBlockException("bad number exception");
      } catch (NonCommonBlockException e) {
        throw new BadBlockException("non common exception");
      } catch (ReceiptCheckErrException e) {
        throw new BadBlockException("OutOfSlotTime Exception");
      } catch (VMIllegalException e) {
        throw new BadBlockException(e.getMessage());
      } catch (UnLinkedBlockException e) {
        throw new BadBlockException("unLink exception");
      }
    }
    logger.info("Sync with trust node completed!!!");
  }

  private void start(Args cfgArgs) {
    lastSolidityBlockNum.set(dbManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber());
    syncExecutor.scheduleWithFixedDelay(() -> {
      initGrpcClient(cfgArgs.getTrustNodeAddr());
      syncSolidityBlock(cfgArgs.getTrustNodeAddr());
      shutdownGrpcClient();
    }, 2500, 5000, TimeUnit.MILLISECONDS);
    syncExecutor.scheduleWithFixedDelay(() -> {
      try {
        processSolidityChain();
      } catch (Throwable t) {
        logger.error("Error in sync solidity block " + t.getMessage());
        lastSolidityBlockNum
            .set(dbManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber());
      }
    }, 5000, 5000, TimeUnit.MILLISECONDS);
  }

  /**
   * Start the SolidityNode.
   */
  public static void main(String[] args) throws InterruptedException {
    logger.info("Solidity node running.");
    Args.setParam(args, Constant.TESTNET_CONF);
    Args cfgArgs = Args.getInstance();

    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
        .getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.toLevel(cfgArgs.getLogLevel()));

    if (StringUtils.isEmpty(cfgArgs.getTrustNodeAddr())) {
      logger.error("Trust node not set.");
      return;
    }
    cfgArgs.setSolidityNode(true);

    ApplicationContext context = new TronApplicationContext(DefaultConfig.class);

    if (cfgArgs.isHelp()) {
      logger.info("Here is the help message.");
      return;
    }
    Application appT = ApplicationFactory.create(context);
    FullNode.shutdown(appT);

    //appT.init(cfgArgs);
    RpcApiService rpcApiService = context.getBean(RpcApiService.class);
    appT.addService(rpcApiService);
    //http
    SolidityNodeHttpApiService httpApiService = context.getBean(SolidityNodeHttpApiService.class);
    appT.addService(httpApiService);

    appT.initServices(cfgArgs);
    appT.startServices();

    //Disable peer discovery for solidity node
    DiscoverServer discoverServer = context.getBean(DiscoverServer.class);
    discoverServer.close();
    ChannelManager channelManager = context.getBean(ChannelManager.class);
    channelManager.close();
    NodeManager nodeManager = context.getBean(NodeManager.class);
    nodeManager.close();

    SolidityNode node = new SolidityNode();
    node.setDbManager(appT.getDbManager());
    node.start(cfgArgs);

    rpcApiService.blockUntilShutdown();
  }
}
