import { defineConfig } from 'vitepress';
import { withMermaid } from 'vitepress-plugin-mermaid';

export default withMermaid(
  defineConfig({
    title: 'Consensus Landscape',
    description: 'Интерактивный симулятор алгоритмов консенсуса',
    lang: 'ru-RU',
    base: process.env.DOCS_BASE ?? '/consensus-landscape/docs/',
    cleanUrls: true,
    lastUpdated: true,

    themeConfig: {
      nav: [
        { text: 'Главная', link: '/' },
        { text: 'Алгоритмы', link: '/overview' },
        { text: 'Модель симуляции', link: '/simulation-model' },
        { text: 'Симулятор', link: 'https://consensus.khorost.tech/' },
        { text: 'GitHub', link: 'https://github.com/khorost-tech/consensus-landscape' },
      ],

      search: {
        provider: 'local',
        options: {
          translations: {
            button: {
              buttonText: 'Поиск',
              buttonAriaLabel: 'Поиск по документации',
            },
            modal: {
              noResultsText: 'Ничего не найдено',
              resetButtonTitle: 'Очистить поиск',
              footer: {
                selectText: 'выбрать',
                navigateText: 'перейти',
                closeText: 'закрыть',
              },
            },
          },
        },
      },

      sidebar: [
        {
          text: 'Старт',
          collapsed: false,
          items: [
            { text: 'О проекте', link: '/' },
            { text: 'Что Такое Консенсус', link: '/overview' },
            { text: 'Модель Симуляции', link: '/simulation-model' },
          ],
        },
        {
          text: 'Алгоритмы',
          collapsed: false,
          items: [
            { text: 'Raft', link: '/algorithms/raft' },
            { text: 'Basic Paxos', link: '/algorithms/paxos' },
            { text: 'Multi-Paxos', link: '/algorithms/multi-paxos' },
            { text: 'Zab', link: '/algorithms/zab' },
            { text: 'EPaxos', link: '/algorithms/epaxos' },
          ],
        },
        {
          text: 'Дальше',
          collapsed: true,
          items: [
            { text: 'Roadmap', link: '/roadmap' },
            { text: 'Другие Алгоритмы', link: '/other-algorithms' },
          ],
        },
      ],

      outline: { label: 'На этой странице', level: [2, 3] },
      docFooter: { prev: 'Назад', next: 'Далее' },
      lastUpdated: {
        text: 'Обновлено',
        formatOptions: {
          dateStyle: 'medium',
          timeStyle: 'short',
        },
      },
      editLink: {
        pattern: 'https://github.com/khorost-tech/consensus-landscape/edit/main/docs/:path',
        text: 'Предложить правку на GitHub',
      },
      footer: {
        message: 'Документация проекта Consensus Landscape',
        copyright: 'MIT License',
      },

      socialLinks: [
        { icon: 'github', link: 'https://github.com/khorost-tech/consensus-landscape' },
      ],
    },

    ignoreDeadLinks: [
      /\/consensus-landscape\/(?:index)?$/,
    ],

    mermaid: {},
  })
);
