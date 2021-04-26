package examples;

@ExtendWith(ArquillianExtension.class)
public class ResourceAdapterTest {

    @Deployment
    public static EnterpriseArchive deploy() throws Exception {

        JavaArchive rarjar = ShrinkWrap.create(JavaArchive.class, "pulasr.jar")
                .addClasses(
                        Create.class,
                        Modify.class,
                        Delete.class,
                        FSWatcher.class,
                        FSWatchingThread.class,
                        FSWatcherResourceAdapter.class,
                        FSWatcherActivationSpec.class);

        ResourceAdapterArchive rar = ShrinkWrap.create(ResourceAdapterArchive.class, "fswatcher.rar")
                .addAsLibrary(rarjar);

        JavaArchive ejbjar = ShrinkWrap.create(JavaArchive.class, "ejb.jar")
                .addClasses(
                        FileEvent.class,
                        FSWatcherMDB.class)
                .addAsManifestResource("jboss-ejb3.xml")
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");

        JavaArchive libjar = ShrinkWrap.create(JavaArchive.class, "lib.jar")
                .addClasses(ResourceAdapterTest.class)
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");

        return ShrinkWrap.create(EnterpriseArchive.class, "test.ear")
                .addAsModules(
                        rar,
                        ejbjar)
                .addAsLibraries(libjar);

    }

    private static CyclicBarrier barrier;

    private static File newFile;

    private static int mode;

    @Before
    public void init() throws Exception {
        newFile = null;
        mode = 0;
        barrier = new CyclicBarrier(2);
        Thread.sleep(1000);
    }

    @Test
    @InSequence(1)
    public void testTxtFile() throws Exception {

        File tempFile = new File(".", "testFile.txt");
        assertTrue("Could not create temp file", tempFile.createNewFile());

        barrier.await(10, TimeUnit.SECONDS);

        assertEquals(tempFile.getName(), newFile.getName());
        assertEquals(FileEvent.CREATE, mode);
    }