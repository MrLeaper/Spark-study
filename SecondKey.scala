class SecondKey(val first:Int,val second:Int) extends Ordered[SecondKey] with Serializable {
  override def compare(that: SecondKey): Int = {
    if(this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}
