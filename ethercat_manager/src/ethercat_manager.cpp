/*
 * Copyright (C) 2015, Jonathan Meyer
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the names of Tokyo Opensource Robotics Kyokai Association. nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

// copied from https://github.com/ros-industrial/robotiq/blob/jade-devel/robotiq_ethercat/src/ethercat_manager.cpp


#include "ethercat_manager/ethercat_manager.h"

#ifndef _WIN32
#include <unistd.h>
#endif
#include <stdio.h>
#include <time.h>

#include <boost/ref.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

#include <soem/ethercattype.h>
#include <soem/nicdrv.h>
#include <soem/ethercatbase.h>
#include <soem/ethercatmain.h>
#include <soem/ethercatdc.h>
#include <soem/ethercatcoe.h>
#include <soem/ethercatfoe.h>
#include <soem/ethercatconfig.h>
#include <soem/ethercatprint.h>

#include <windows.h>

namespace 
{
static const unsigned THREAD_SLEEP_TIME = 1000; // 1 ms
static const unsigned EC_TIMEOUTMON = 500;
static const int NSEC_PER_SECOND = 1e+9;
void timespecInc(struct timespec &tick, int nsec)
{
  tick.tv_nsec += nsec;
  while (tick.tv_nsec >= NSEC_PER_SECOND)
    {
      tick.tv_nsec -= NSEC_PER_SECOND;
      tick.tv_sec++;
    }
}

#define CLOCK_REALTIME 0
#define TIMER_ABSTIME 0

LARGE_INTEGER
getFILETIMEoffset()
{
  SYSTEMTIME s;
  FILETIME f;
  LARGE_INTEGER t;

  s.wYear = 1970;
  s.wMonth = 1;
  s.wDay = 1;
  s.wHour = 0;
  s.wMinute = 0;
  s.wSecond = 0;
  s.wMilliseconds = 0;
  SystemTimeToFileTime(&s, &f);
  t.QuadPart = f.dwHighDateTime;
  t.QuadPart <<= 32;
  t.QuadPart |= f.dwLowDateTime;
  return (t);
}

int
clock_gettime(int X, struct timespec *tv)
{
  LARGE_INTEGER           t;
  FILETIME            f;
  double                  microseconds;
  static LARGE_INTEGER    offset;
  static double           frequencyToMicroseconds;
  static int              initialized = 0;
  static BOOL             usePerformanceCounter = 0;

  if (!initialized) {
    LARGE_INTEGER performanceFrequency;
    initialized = 1;
    usePerformanceCounter = QueryPerformanceFrequency(&performanceFrequency);
    if (usePerformanceCounter) {
      QueryPerformanceCounter(&offset);
      frequencyToMicroseconds = (double)performanceFrequency.QuadPart / 1000000.;
    } else {
      offset = getFILETIMEoffset();
      frequencyToMicroseconds = 10.;
    }
  }
  if (usePerformanceCounter) QueryPerformanceCounter(&t);
  else {
    GetSystemTimeAsFileTime(&f);
    t.QuadPart = f.dwHighDateTime;
    t.QuadPart <<= 32;
    t.QuadPart |= f.dwLowDateTime;
  }

  t.QuadPart -= offset.QuadPart;
  microseconds = (double)t.QuadPart / frequencyToMicroseconds;
  t.QuadPart = microseconds;
  tv->tv_sec = t.QuadPart / 1000000;
  tv->tv_nsec = t.QuadPart % 1000000 * 1000;
  return (0);
}

int clock_nanosleep(int clock_id, int flags,
       const struct timespec *rqtp, struct timespec *rmtp)
{
  Sleep(1);
  return 0;
}

void handleErrors()
{
  /* one ore more slaves are not responding */
  ec_group[0].docheckstate = FALSE;
  ec_readstate();
  for (int slave = 1; slave <= ec_slavecount; slave++)
  {
    if ((ec_slave[slave].group == 0) && (ec_slave[slave].state != EC_STATE_OPERATIONAL))
    {
      ec_group[0].docheckstate = TRUE;
      if (ec_slave[slave].state == (EC_STATE_SAFE_OP + EC_STATE_ERROR))
      {
        fprintf(stderr, "ERROR : slave %d is in SAFE_OP + ERROR, attempting ack.\n", slave);
        ec_slave[slave].state = (EC_STATE_SAFE_OP + EC_STATE_ACK);
        ec_writestate(slave);
      }
      else if(ec_slave[slave].state == EC_STATE_SAFE_OP)
      {
        fprintf(stderr, "WARNING : slave %d is in SAFE_OP, change to OPERATIONAL.\n", slave);
        ec_slave[slave].state = EC_STATE_OPERATIONAL;
        ec_writestate(slave);
      }
      else if(ec_slave[slave].state > 0)
      {
        if (ec_reconfig_slave(slave, EC_TIMEOUTMON))
        {
          ec_slave[slave].islost = FALSE;
          printf("MESSAGE : slave %d reconfigured\n",slave);
        }
      }
      else if(!ec_slave[slave].islost)
      {
        /* re-check state */
        ec_statecheck(slave, EC_STATE_OPERATIONAL, EC_TIMEOUTRET);
        if (!ec_slave[slave].state)
        {
          ec_slave[slave].islost = TRUE;
          fprintf(stderr, "ERROR : slave %d lost\n",slave);
        }
      }
    }
    if (ec_slave[slave].islost)
    {
      if(!ec_slave[slave].state)
      {
        if (ec_recover_slave(slave, EC_TIMEOUTMON))
        {
          ec_slave[slave].islost = FALSE;
          printf("MESSAGE : slave %d recovered\n",slave);
        }
      }
      else
      {
        ec_slave[slave].islost = FALSE;
        printf("MESSAGE : slave %d found\n",slave);
      }
    }
  }
}

void cycleWorker(boost::mutex& mutex, bool& stop_flag)
{
  // 1ms in nanoseconds
  double period = THREAD_SLEEP_TIME * 1000;
  // get curren ttime
  struct timespec tick;
  clock_gettime(CLOCK_REALTIME, &tick);
  timespecInc(tick, period);
  while (!stop_flag) 
  {
    int expected_wkc = (ec_group[0].outputsWKC * 2) + ec_group[0].inputsWKC;
    int sent, wkc;
    {
      boost::mutex::scoped_lock lock(mutex);
      sent = ec_send_processdata();
      wkc = ec_receive_processdata(EC_TIMEOUTRET);
    }

    if (wkc < expected_wkc)
    {
      handleErrors();
    }

    // check overrun
    struct timespec before;
    clock_gettime(CLOCK_REALTIME, &before);
    double overrun_time = (before.tv_sec + double(before.tv_nsec)/NSEC_PER_SECOND) -  (tick.tv_sec + double(tick.tv_nsec)/NSEC_PER_SECOND);
    if (overrun_time > 0.0)
    {
      //fprintf(stderr, "  overrun: %f\n", overrun_time);
    }
    //usleep(THREAD_SLEEP_TIME);
    clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &tick, NULL);
    timespecInc(tick, period);
    //printf("%24.12f %14.10f [msec] %02x %02x %02x %02x\n", tick.tv_sec+double(tick.tv_nsec)/NSEC_PER_SECOND, overrun_time*1000, ec_slave[1].outputs[24], ec_slave[1].outputs[23], ec_slave[1].outputs[22], ec_slave[1].outputs[21]);
  }
}

} // end of anonymous namespace


namespace ethercat {

EtherCatManager::EtherCatManager(const std::string& ifname)
  : ifname_(ifname), 
    num_clients_(0),
    stop_flag_(false)
{
  if (initSoem(ifname)) 
  {
    cycle_thread_ = boost::thread(cycleWorker, 
                                  boost::ref(iomap_mutex_),
                                  boost::ref(stop_flag_));
  } 
  else 
 {
   // construction failed
   throw EtherCatError("Could not initialize SOEM");
 }
}

EtherCatManager::~EtherCatManager()
{
  stop_flag_ = true;

  // Request init operational state for all slaves
  ec_slave[0].state = EC_STATE_INIT;

  /* request init state for all slaves */
  ec_writestate(0);

  //stop SOEM, close socket
  ec_close();
  cycle_thread_.join();
}

static int simco_setup(uint16 slave)
{
    /*
    PDO mapping according to CoE :
    SM2 outputs
        addr b   index: sub bitl data_type    name
    [0x0016.0] 0x6040:0x00 0x10 UNSIGNED16   Controlword
    [0x0018.0] 0x607A:0x00 0x20 INTEGER32    Target Position
    [0x001C.0] 0x60FF:0x00 0x20 INTEGER32    Target Velocity
    [0x0020.0] 0x6083:0x00 0x20 UNSIGNED32   Profile Acceleration
    [0x0024.0] 0x6084:0x00 0x20 UNSIGNED32   Profile Deceleration
    [0x0028.0] 0x6086:0x00 0x10 INTEGER16    Motion Profile Type
    [0x002A.0] 0x6060:0x00 0x08 INTEGER8     Modes Of Operation
    [0x002B.0] 0x0000:0x00 0x00
    SM3 inputs
        addr b   index: sub bitl data_type    name
    [0x004A.0] 0x6041:0x00 0x10 UNSIGNED16   Statusword
    [0x004C.0] 0x6064:0x00 0x20 INTEGER32    Position Actual Value
    [0x0050.0] 0x606C:0x00 0x20 INTEGER32    Velocity Actual Value
    [0x0054.0] 0x3609:0x02 0x20 INTEGER32    Iq Actual Filtered
    [0x0058.0] 0x6061:0x00 0x08 INTEGER8     Modes Of Operation Display
    [0x0059.0] 0x3D00:0x02 0x20 UNSIGNED32   Group Error
    */
    uint8 u8val = 0;
    ec_SDOwrite(slave, 0x1c12, 0x00, FALSE, sizeof(u8val), &u8val, EC_TIMEOUTRXM);

    u8val = 0;
    ec_SDOwrite(slave, 0x1c13, 0x00, FALSE, sizeof(u8val), &u8val, EC_TIMEOUTRXM);

    /* Map velocity PDO assignment via Complete Access*/
    uint16 assignment_TxPDO[] = {0x1a0b};
    uint16 assignment_RxPDO[] = {0x160a};

    ec_SDOwrite(4, 0x1C12, 0x01, FALSE, sizeof(assignment_RxPDO), &assignment_RxPDO, EC_TIMEOUTSAFE);
    u8val = 1;
    ec_SDOwrite(slave, 0x1c12, 0x00, FALSE, sizeof(u8val), &u8val, EC_TIMEOUTRXM);

    ec_SDOwrite(4, 0x1C13, 0x01, FALSE, sizeof(assignment_TxPDO), &assignment_TxPDO, EC_TIMEOUTSAFE);
    u8val = 1;
    ec_SDOwrite(slave, 0x1c13, 0x00, FALSE, sizeof(u8val), &u8val, EC_TIMEOUTRXM);
    return 0;
}

#define IF_MINAS(_ec_slave) ((int)_ec_slave.eep_man == 0x010a)
bool EtherCatManager::initSoem(const std::string& ifname) {
  // Copy string contents because SOEM library doesn't 
  // practice const correctness
  const static unsigned MAX_BUFF_SIZE = 1024;
  char buffer[MAX_BUFF_SIZE];
  size_t name_size = ifname_.size();
  if (name_size > sizeof(buffer) - 1) 
  {
    fprintf(stderr, "Ifname %s exceeds maximum size of %u bytes\n", ifname_.c_str(), MAX_BUFF_SIZE);
    return false;
  }
  std::strncpy(buffer, ifname_.c_str(), MAX_BUFF_SIZE);

  printf("Initializing etherCAT master\n");

  if (!ec_init(buffer))
  {
    fprintf(stderr, "Could not initialize ethercat driver\n");
    return false;
  }

  /* find and auto-config slaves */
  if (ec_config_init(FALSE) <= 0)
  {
    fprintf(stderr, "No slaves found on %s\n", ifname_.c_str());
    return false;
  }

  printf("SOEM found and configured %d slaves\n", ec_slavecount);
  for( int cnt = 1 ; cnt <= ec_slavecount ; cnt++)
  {
    // MINAS-A5B Serial Man = 066Fh, ID = [5/D]****[0/4/8][0-F]*
    printf(" Man: %8.8x ID: %8.8x Rev: %8.8x %s\n", (int)ec_slave[cnt].eep_man, (int)ec_slave[cnt].eep_id, (int)ec_slave[cnt].eep_rev, IF_MINAS(ec_slave[cnt])?" MINAS Drivers":"");
    if(IF_MINAS(ec_slave[cnt])) {
      num_clients_++;
    }
  }
  printf("Found %d MINAS Drivers\n", num_clients_);

  if (ec_statecheck(0, EC_STATE_PRE_OP, EC_TIMEOUTSTATE*4) != EC_STATE_PRE_OP)
  {
    fprintf(stderr, "Could not set EC_STATE_PRE_OP\n");
    return false;
  }

  for( int cnt = 1 ; cnt <= ec_slavecount ; cnt++)
  {
    if (! IF_MINAS(ec_slave[cnt])) continue;

    ec_slave[cnt].PO2SOconfig = &simco_setup;
  }

  // configure IOMap
  int iomap_size = ec_config_map(iomap_);
  printf("SOEM IOMap size: %d\n", iomap_size);

  // locates dc slaves - ???
  ec_configdc();

  // '0' here addresses all slaves
  if (ec_statecheck(0, EC_STATE_SAFE_OP, EC_TIMEOUTSTATE*4) != EC_STATE_SAFE_OP)
  {
    fprintf(stderr, "Could not set EC_STATE_SAFE_OP\n");
    return false;
  }

  /* 
      This section attempts to bring all slaves to operational status. It does so
      by attempting to set the status of all slaves (ec_slave[0]) to operational,
      then proceeding through 40 send/recieve cycles each waiting up to 50 ms for a
      response about the status. 
  */
  ec_slave[0].state = EC_STATE_OPERATIONAL;
  ec_send_processdata();
  ec_receive_processdata(EC_TIMEOUTRET);

  ec_writestate(0);
  int chk = 40;
  do {
    ec_send_processdata();
    ec_receive_processdata(EC_TIMEOUTRET);
    ec_statecheck(0, EC_STATE_OPERATIONAL, 50000); // 50 ms wait for state check
  } while (chk-- && (ec_slave[0].state != EC_STATE_OPERATIONAL));

  if(ec_statecheck(0,EC_STATE_OPERATIONAL, EC_TIMEOUTSTATE) != EC_STATE_OPERATIONAL)
  {
    fprintf(stderr, "OPERATIONAL state not set, exiting\n");
    return false;
  }

  ec_readstate();
  for( int cnt = 1 ; cnt <= ec_slavecount ; cnt++)
    {
      if (! IF_MINAS(ec_slave[cnt])) continue;
      printf("\nSlave:%d\n Name:%s\n Output size: %dbits\n Input size: %dbits\n State: %d\n Delay: %d[ns]\n Has DC: %d\n",
	     cnt, ec_slave[cnt].name, ec_slave[cnt].Obits, ec_slave[cnt].Ibits,
	     ec_slave[cnt].state, ec_slave[cnt].pdelay, ec_slave[cnt].hasdc);
      if (ec_slave[cnt].hasdc) printf(" DCParentport:%d\n", ec_slave[cnt].parentport);
      printf(" Activeports:%d.%d.%d.%d\n", (ec_slave[cnt].activeports & 0x01) > 0 ,
	     (ec_slave[cnt].activeports & 0x02) > 0 , 
	     (ec_slave[cnt].activeports & 0x04) > 0 , 
	     (ec_slave[cnt].activeports & 0x08) > 0 );
      printf(" Configured address: %4.4x\n", ec_slave[cnt].configadr);
    }

  for( int cnt = 1 ; cnt <= ec_slavecount ; cnt++)
    {
      if (! IF_MINAS(ec_slave[cnt])) continue;
      int ret = 0, l;
      uint16_t sync_mode;
      uint32_t cycle_time;
      uint32_t minimum_cycle_time;
      uint32_t sync0_cycle_time;
      l = sizeof(sync_mode);
      ret += ec_SDOread(cnt, 0x1c32, 0x01, FALSE, &l, &sync_mode, EC_TIMEOUTRXM);
      l = sizeof(cycle_time);
      ret += ec_SDOread(cnt, 0x1c32, 0x02, FALSE, &l, &cycle_time, EC_TIMEOUTRXM);
      l = sizeof(minimum_cycle_time);
      ret += ec_SDOread(cnt, 0x1c32, 0x05, FALSE, &l, &minimum_cycle_time, EC_TIMEOUTRXM);
      l = sizeof(sync0_cycle_time);
      ret += ec_SDOread(cnt, 0x1c32, 0x0a, FALSE, &l, &sync0_cycle_time, EC_TIMEOUTRXM);
      printf("PDO syncmode %02x, cycle time %d ns (min %d), sync0 cycle time %d ns, ret = %d\n", sync_mode, cycle_time, minimum_cycle_time, sync0_cycle_time, ret);
    }

  printf("\nFinished configuration successfully\n");
  return true;
}

int EtherCatManager::getNumClinets() const
{
  return num_clients_;
}

void EtherCatManager::getStatus(int slave_no, std::string &name, int &eep_man, int &eep_id, int &eep_rev, int &obits, int &ibits, int &state, int &pdelay, int &hasdc, int &activeports, int &configadr) const
{
  if (slave_no > ec_slavecount) {
    fprintf(stderr, "ERROR : slave_no(%d) is larger than ec_slavecount(%d)\n", slave_no, ec_slavecount);
    exit(1);
  }
  name = std::string(ec_slave[slave_no].name);
  eep_man = (int)ec_slave[slave_no].eep_man;
  eep_id  = (int)ec_slave[slave_no].eep_id;
  eep_rev = (int)ec_slave[slave_no].eep_rev;
  obits   = ec_slave[slave_no].Obits;
  ibits   = ec_slave[slave_no].Ibits;
  state   = ec_slave[slave_no].state;
  pdelay  = ec_slave[slave_no].pdelay;
  hasdc   = ec_slave[slave_no].hasdc;
  activeports = ec_slave[slave_no].activeports;
  configadr   = ec_slave[slave_no].configadr;
}

void EtherCatManager::write(int slave_no, uint8_t channel, uint8_t value)
{
  boost::mutex::scoped_lock lock(iomap_mutex_);
  ec_slave[slave_no].outputs[channel] = value;
}

uint8_t EtherCatManager::readInput(int slave_no, uint8_t channel) const
{
  boost::mutex::scoped_lock lock(iomap_mutex_);
  if (slave_no > ec_slavecount) {
    fprintf(stderr, "ERROR : slave_no(%d) is larger than ec_slavecount(%d)\n", slave_no, ec_slavecount);
    exit(1);
  }
  //if (channel*8 >= ec_slave[slave_no].Ibits) {
  //  fprintf(stderr, "ERROR : channel(%d) is larget thatn Input bits (%d)\n", channel*8, ec_slave[slave_no].Ibits);
  //  exit(1);
  //}
  return ec_slave[slave_no].inputs[channel];
}

uint8_t EtherCatManager::readOutput(int slave_no, uint8_t channel) const
{
  boost::mutex::scoped_lock lock(iomap_mutex_);
  if (slave_no > ec_slavecount) {
    fprintf(stderr, "ERROR : slave_no(%d) is larger than ec_slavecount(%d)\n", slave_no, ec_slavecount);
    exit(1);
  }
  //if (channel*8 >= ec_slave[slave_no].Obits) {
  //  fprintf(stderr, "ERROR : channel(%d) is larget thatn Output bits (%d)\n", channel*8, ec_slave[slave_no].Obits);
  //  exit(1);
  //}
  return ec_slave[slave_no].outputs[channel];
}

template <typename T>
uint8_t EtherCatManager::writeSDO(int slave_no, uint16_t index, uint8_t subidx, T value) const
{
  int ret;
  ret = ec_SDOwrite(slave_no, index, subidx, FALSE, sizeof(value), &value, EC_TIMEOUTSAFE);
  return ret;
}

template <typename T>
T EtherCatManager::readSDO(int slave_no, uint16_t index, uint8_t subidx) const
{
  int ret, l;
  T val;
  l = sizeof(val);
  ret = ec_SDOread(slave_no, index, subidx, FALSE, &l, &val, EC_TIMEOUTRXM);
  if ( ret <= 0 ) { // ret = Workcounter from last slave response
    fprintf(stderr, "Failed to read from ret:%d, slave_no:%d, index:0x%04x, subidx:0x%02x\n", ret, slave_no, index, subidx);
  }
  return val;
}

template uint8_t EtherCatManager::writeSDO<char> (int slave_no, uint16_t index, uint8_t subidx, char value) const;
template uint8_t EtherCatManager::writeSDO<int> (int slave_no, uint16_t index, uint8_t subidx, int value) const;
template uint8_t EtherCatManager::writeSDO<short> (int slave_no, uint16_t index, uint8_t subidx, short value) const;
template uint8_t EtherCatManager::writeSDO<long> (int slave_no, uint16_t index, uint8_t subidx, long value) const;
template uint8_t EtherCatManager::writeSDO<unsigned char> (int slave_no, uint16_t index, uint8_t subidx, unsigned char value) const;
template uint8_t EtherCatManager::writeSDO<unsigned int> (int slave_no, uint16_t index, uint8_t subidx, unsigned int value) const;
template uint8_t EtherCatManager::writeSDO<unsigned short> (int slave_no, uint16_t index, uint8_t subidx, unsigned short value) const;
template uint8_t EtherCatManager::writeSDO<unsigned long> (int slave_no, uint16_t index, uint8_t subidx, unsigned long value) const;

template char EtherCatManager::readSDO<char> (int slave_no, uint16_t index, uint8_t subidx) const;
template int EtherCatManager::readSDO<int> (int slave_no, uint16_t index, uint8_t subidx) const;
template short EtherCatManager::readSDO<short> (int slave_no, uint16_t index, uint8_t subidx) const;
template long EtherCatManager::readSDO<long> (int slave_no, uint16_t index, uint8_t subidx) const;
template unsigned char EtherCatManager::readSDO<unsigned char> (int slave_no, uint16_t index, uint8_t subidx) const;
template unsigned int EtherCatManager::readSDO<unsigned int> (int slave_no, uint16_t index, uint8_t subidx) const;
template unsigned short EtherCatManager::readSDO<unsigned short> (int slave_no, uint16_t index, uint8_t subidx) const;
template unsigned long EtherCatManager::readSDO<unsigned long> (int slave_no, uint16_t index, uint8_t subidx) const;

}

